/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package ordering

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// BFT broadcaster histograms exported via the OTel global meter. Metric names
// follow the dotted convention; the Prometheus / VictoriaMetrics exporter
// converts dots to underscores, so consumers query e.g.
//
//	cbdc_ordering_bft_broadcast_duration_bucket
//
// Phase names mirror cbdc-biz/pkg/observability:
//   - broadcast: end-to-end BFTBroadcaster.Broadcast call (includes retries)
//   - get_connection: bftOrdererState pool acquire (channel pop or new conn)
//   - send_recv: per-orderer SendAndRecv round-trip (Send + Recv wait)
//   - recv_only: Recv portion only (helps separate orderer ack latency from
//     local gRPC Send cost)
const (
	scopeOrdering = "fabric-smart-client.ordering"

	metricBroadcast     = "cbdc.ordering.bft.broadcast.duration"
	metricGetConnection = "cbdc.ordering.bft.get_connection.duration"
	metricSendRecv      = "cbdc.ordering.bft.send_recv.duration"
	metricRecvOnly      = "cbdc.ordering.bft.recv_only.duration"
	// get_connection sub-phases: which acquire stage served the request.
	metricConnPoolHit = "cbdc.ordering.bft.conn_pool_hit.duration"
	metricConnDial    = "cbdc.ordering.bft.conn_dial.duration"
	metricConnWait    = "cbdc.ordering.bft.conn_wait.duration"
	metricConnDiscard = "cbdc.ordering.bft.conn_discard.total"
	metricPoolIdle    = "cbdc.ordering.bft.pool_idle"
)

// conn_discard reasons.
const (
	bftDiscardSendErr   = "send_err"
	bftDiscardTimeout   = "timeout"
	bftDiscardStatusErr = "status_err"
	bftDiscardPoolFull  = "pool_full"
)

// bftPhaseBuckets cover the latency distribution observed in c=100 — c=800
// stress: from sub-ms (cached connection acquire) to ~10s (BFT 5s batch tick
// plus retries). Same boundaries as cbdc-biz pkg/observability PhaseBuckets
// to keep cross-component Grafana queries consistent.
var bftPhaseBuckets = []float64{
	0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10,
}

var (
	bftMetricsOnce sync.Once
	bftBroadcast   metric.Float64Histogram
	bftGetConn     metric.Float64Histogram
	bftSendRecv    metric.Float64Histogram
	bftRecvOnly    metric.Float64Histogram
	bftConnPoolHit metric.Float64Histogram
	bftConnDial    metric.Float64Histogram
	bftConnWait    metric.Float64Histogram
	bftConnDiscard metric.Int64Counter
)

// ensureBFTMetricsInit lazily registers BFT histograms with the OTel global
// meter. Runs at first call — before the OTel SDK is wired, otel.Meter
// returns a no-op provider, so registrations are valid but emit nothing.
// After Setup, instruments cached here keep working since the SDK swaps the
// internal pipeline, not the meter handle.
//
// Idempotent and concurrency-safe.
func ensureBFTMetricsInit() {
	bftMetricsOnce.Do(func() {
		meter := otel.Meter(scopeOrdering)

		bftBroadcast = newBFTHistogram(meter, metricBroadcast,
			"End-to-end BFTBroadcaster.Broadcast call (includes retries).")
		bftGetConn = newBFTHistogram(meter, metricGetConnection,
			"Time spent acquiring a connection from the per-orderer pool.")
		bftSendRecv = newBFTHistogram(meter, metricSendRecv,
			"Per-orderer SendAndRecv round-trip (Send + Recv wait).")
		bftRecvOnly = newBFTHistogram(meter, metricRecvOnly,
			"Recv-only portion of SendAndRecv (orderer ack latency).")
		bftConnPoolHit = newBFTHistogram(meter, metricConnPoolHit,
			"Connection served from the pool without waiting.")
		bftConnDial = newBFTHistogram(meter, metricConnDial,
			"New orderer connection dial (TCP+TLS handshake+stream).")
		bftConnWait = newBFTHistogram(meter, metricConnWait,
			"Blocking wait for a pooled connection or a free slot.")
		if c, err := meter.Int64Counter(metricConnDiscard,
			metric.WithDescription("Orderer connections destroyed, by reason.")); err == nil {
			bftConnDiscard = c
		}
	})
}

// recordBFTDiscard counts a destroyed orderer connection with its reason.
func recordBFTDiscard(ctx context.Context, orderer, reason string) {
	if bftConnDiscard == nil {
		return
	}
	bftConnDiscard.Add(ctx, 1, metric.WithAttributes(
		attribute.String("orderer", orderer),
		attribute.String("reason", reason),
	))
}

// registerBFTPoolGauge exports per-orderer idle pooled connections for the
// given broadcaster. One callback per broadcaster instance; in practice a
// process runs a single broadcaster per ordering service.
func registerBFTPoolGauge(b *BFTBroadcaster) {
	ensureBFTMetricsInit()
	meter := otel.Meter(scopeOrdering)
	gauge, err := meter.Int64ObservableGauge(metricPoolIdle,
		metric.WithDescription("Idle pooled orderer connections."))
	if err != nil {
		return
	}
	_, _ = meter.RegisterCallback(func(_ context.Context, obs metric.Observer) error {
		b.statesLock.RLock()
		defer b.statesLock.RUnlock()
		for addr, state := range b.states {
			obs.ObserveInt64(gauge, int64(len(state.pool)),
				metric.WithAttributes(attribute.String("orderer", addr)))
		}
		return nil
	}, gauge)
}

func newBFTHistogram(meter metric.Meter, name, desc string) metric.Float64Histogram {
	h, err := meter.Float64Histogram(name,
		metric.WithUnit("s"),
		metric.WithDescription(desc),
		metric.WithExplicitBucketBoundaries(bftPhaseBuckets...),
	)
	if err != nil {
		// Registration only fails on duplicate-name conflicts. Return a
		// no-op so the instrumented code path stays functional.
		return bftNoopHistogram{}
	}
	return h
}

// recordBFTPhase records a duration on the given histogram with error label.
// Mirrors cbdc-biz observability.RecordPhase but without the tracing/counter
// pieces — the BFT path is hot and we keep the per-call overhead minimal.
func recordBFTPhase(ctx context.Context, h metric.Float64Histogram, elapsed time.Duration, err error, orderer string) {
	if h == nil {
		return
	}
	h.Record(ctx, elapsed.Seconds(), metric.WithAttributes(
		attribute.Bool("error", err != nil),
		attribute.String("orderer", orderer),
	))
}

// bftNoopHistogram drops all observations silently. Used when meter
// registration fails (duplicate name, etc.) so callers don't need to
// guard each Record call.
type bftNoopHistogram struct{ metric.Float64Histogram }

func (bftNoopHistogram) Record(context.Context, float64, ...metric.RecordOption) {}

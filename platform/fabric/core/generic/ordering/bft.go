/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package ordering

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	common2 "github.com/hyperledger/fabric-protos-go-apiv2/common"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc/status"

	"github.com/hyperledger-labs/fabric-smart-client/pkg/utils/errors"
	fscmetrics "github.com/hyperledger-labs/fabric-smart-client/platform/fabric/core/generic/metrics"
	"github.com/hyperledger-labs/fabric-smart-client/platform/fabric/driver"
	"github.com/hyperledger-labs/fabric-smart-client/platform/view/services/grpc"
)

// OTel instruments for BFT broadcast diagnostics. Names mirror the
// cbdc.view.phase.duration convention used in cbdc-biz/pkg/observability so
// VictoriaMetrics queries can join across phases. Attributes kept at low
// cardinality (outcome only); orderer address is intentionally omitted.
const bftMetricsScope = "cbdc.ordering.bft"

var (
	bftDurationBuckets = []float64{
		0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10,
	}
	bftRetryBuckets = []float64{0, 1, 2, 3, 5, 10}
)

var (
	bftMetricsInitOnce   sync.Once
	bftBroadcastDuration metric.Float64Histogram
	bftBroadcastRetries  metric.Float64Histogram
	bftGetConnectionDur  metric.Float64Histogram
	bftSendRecvDuration  metric.Float64Histogram
)

func ensureBFTMetricsInit() {
	bftMetricsInitOnce.Do(func() {
		meter := otel.Meter(bftMetricsScope)
		var err error
		bftBroadcastDuration, err = meter.Float64Histogram(
			bftMetricsScope+".broadcast.duration",
			metric.WithUnit("s"),
			metric.WithDescription("BFT broadcast end-to-end duration"),
			metric.WithExplicitBucketBoundaries(bftDurationBuckets...),
		)
		if err != nil {
			bftBroadcastDuration = noopBFTHistogram{}
		}
		bftBroadcastRetries, err = meter.Float64Histogram(
			bftMetricsScope+".broadcast.retries",
			metric.WithDescription("BFT broadcast retry count (0 = first attempt succeeded)"),
			metric.WithExplicitBucketBoundaries(bftRetryBuckets...),
		)
		if err != nil {
			bftBroadcastRetries = noopBFTHistogram{}
		}
		bftGetConnectionDur, err = meter.Float64Histogram(
			bftMetricsScope+".get_connection.duration",
			metric.WithUnit("s"),
			metric.WithDescription("BFT getConnection duration (single orderer)"),
			metric.WithExplicitBucketBoundaries(bftDurationBuckets...),
		)
		if err != nil {
			bftGetConnectionDur = noopBFTHistogram{}
		}
		bftSendRecvDuration, err = meter.Float64Histogram(
			bftMetricsScope+".send_recv.duration",
			metric.WithUnit("s"),
			metric.WithDescription("BFT per-orderer Send+Recv round-trip duration"),
			metric.WithExplicitBucketBoundaries(bftDurationBuckets...),
		)
		if err != nil {
			bftSendRecvDuration = noopBFTHistogram{}
		}
	})
}

// noopBFTHistogram satisfies metric.Float64Histogram when registration fails
// (e.g. duplicate name) so the instrumented code path stays functional.
type noopBFTHistogram struct{ metric.Float64Histogram }

func (noopBFTHistogram) Record(context.Context, float64, ...metric.RecordOption) {}

type BFTBroadcaster struct {
	ConfigService driver.ConfigService
	ClientFactory Services

	connSem  *semaphore.Weighted
	metrics  *fscmetrics.Metrics
	poolSize int

	connectionsLock sync.RWMutex
	connections     map[string]chan *Connection
}

func NewBFTBroadcaster(configService driver.ConfigService, cf Services, metrics *fscmetrics.Metrics) *BFTBroadcaster {
	return &BFTBroadcaster{
		ConfigService: configService,
		ClientFactory: cf,
		connections:   map[string]chan *Connection{},
		connSem:       semaphore.NewWeighted(int64(configService.OrdererConnectionPoolSize())),
		metrics:       metrics,
		poolSize:      configService.OrdererConnectionPoolSize(),
	}
}

func (o *BFTBroadcaster) Broadcast(ctx context.Context, env *common2.Envelope) error {
	ensureBFTMetricsInit()
	bcastStart := time.Now()
	logger.DebugfContext(ctx, "Start BFT Broadcast")
	defer logger.DebugfContext(ctx, "End BFT Broadcast")
	// send the envelope for ordering
	retries := o.ConfigService.BroadcastNumRetries()
	retryInterval := o.ConfigService.BroadcastRetryInterval()
	orderers := o.ConfigService.Orderers()
	if len(orderers) < 4 {
		bftBroadcastDuration.Record(ctx, time.Since(bcastStart).Seconds(),
			metric.WithAttributes(attribute.String("outcome", "config_err")))
		return errors.Errorf("not enough orderers, 4 minimum got [%d]", len(orderers))
	}

	n := len(orderers)
	f := (int(n) - 1) / 3
	threshold := int(math.Ceil((float64(n) + float64(f) + 1) / 2.0))

	for i := range retries {
		if i > 0 {
			logger.DebugfContext(ctx, "broadcast, retry [%d]...", i)
			// wait a bit
			time.Sleep(retryInterval)
		}

		counter := 0
		var errs []error
		var usedConnections []*Connection

		var wg sync.WaitGroup
		wg.Add(n)

		var lock sync.Mutex

		for _, orderer := range orderers {
			go func(orderer *grpc.ConnectionConfig) {
				defer wg.Done()

				logger.DebugfContext(ctx, "get connection to [%s]", orderer.Address)
				connection, err := o.getConnection(ctx, orderer)

				lock.Lock()
				if err != nil {
					errs = append(errs, errors.Wrapf(err, "failed connecting to [%v]", orderer.Address))
					logger.WarnfContext(ctx, "failed to get connection to orderer [%s]", orderer.Address, err)
					lock.Unlock()
					return
				}

				lock.Unlock()

				logger.DebugfContext(ctx, "broadcast to [%s]", orderer.Address)
				sendRecvStart := time.Now()
				recordSendRecv := func(outcome string) {
					bftSendRecvDuration.Record(ctx, time.Since(sendRecvStart).Seconds(),
						metric.WithAttributes(attribute.String("outcome", outcome)))
				}
				err = connection.Send(env)
				if err != nil {
					logger.ErrorfContext(ctx, "failed to broadcast to [%s]: %s", orderer.Address, err.Error())
					recordSendRecv("send_err")
					lock.Lock()
					defer lock.Unlock()
					usedConnections = append(usedConnections, connection)
					return
				}
				status, err := connection.Recv()
				if err != nil {
					logger.ErrorfContext(ctx, "failed to get status after broadcast to [%s]: %s", orderer.Address, err.Error())
					recordSendRecv("recv_err")
					lock.Lock()
					defer lock.Unlock()
					usedConnections = append(usedConnections, connection)
					return
				}

				lock.Lock()
				defer lock.Unlock()

				switch status.GetStatus() {
				case common2.Status_SUCCESS:
					recordSendRecv("success")
					o.releaseConnection(connection, orderer)
					counter++
				default:
					recordSendRecv("status_failure")
					usedConnections = append(usedConnections, connection)
					logger.ErrorfContext(ctx, "failed to get status after broadcast to [%s]: %s", orderer.Address, common2.Status_name[int32(status.GetStatus())])
					errs = append(errs, fmt.Errorf("failed to get status after broadcast to [%s]: %s", orderer.Address, common2.Status_name[int32(status.GetStatus())]))
					return
				}
			}(orderer)
		}

		wg.Wait()

		// did we send to enough orderers?
		// if not, discard all connections
		if counter >= threshold {
			// success
			bftBroadcastRetries.Record(ctx, float64(i))
			bftBroadcastDuration.Record(ctx, time.Since(bcastStart).Seconds(),
				metric.WithAttributes(attribute.String("outcome", "success")))
			return nil
		}

		// fail
		logger.WarnfContext(ctx, "failed to broadcast, got [%d of %d] success and errs [%v], retry after a delay", counter, threshold, errs)
		// cleanup connections
		for _, connection := range usedConnections {
			o.discardConnection(connection)
		}
	}

	bftBroadcastRetries.Record(ctx, float64(retries))
	bftBroadcastDuration.Record(ctx, time.Since(bcastStart).Seconds(),
		metric.WithAttributes(attribute.String("outcome", "failure")))
	return errors.Errorf("failed to send transaction to the orderering service")
}

// getConnection acquires a Connection for the given orderer using a
// non-spinning, three-stage selection.
//
// Background: the previous implementation used a `for { select { case <-pool: ;
// default: Acquire(timeout=1s) } }` spin loop. Under high concurrent demand
// this triggered Go's mutex starvation mode on the Weighted semaphore's
// internal mutex: hundreds of goroutines retried Acquire+timeout in a tight
// loop, none successfully completed the critical section, the pool stayed
// empty forever (a positive-feedback livelock). Confirmed by goroutine pprof
// at pool=30 c=50/c=400 c=800 — 0 connections ever created, all goroutines
// stuck at semaphore.go:41 (s.mu.Lock).
//
// New design has no busy retry. Each call locks the sem mutex at most once
// (via TryAcquire) and then either takes a fast path or blocks on the pool
// channel until a peer broadcast releases. Mutex throughput requirement is
// proportional to arrival rate, not retry rate — never enters starvation mode.
func (o *BFTBroadcaster) getConnection(ctx context.Context, to *grpc.ConnectionConfig) (*Connection, error) {
	ensureBFTMetricsInit()
	getConnStart := time.Now()
	outcome := "unknown"
	defer func() {
		attrs := metric.WithAttributes(attribute.String("outcome", outcome))
		bftGetConnectionDur.Record(ctx, time.Since(getConnStart).Seconds(), attrs)
	}()
	pool := o.connectionPool(to.Address)

	// Fast path 1: an idle connection is already in the pool — take it
	// without blocking. The default branch covers the empty-pool case.
	select {
	case connection := <-pool:
		outcome = "pool_hit"
		return connection, nil
	default:
	}

	// Fast path 2: pool empty but the semaphore allows creating a new
	// connection. TryAcquire is a single mutex op (no waiters list, no
	// timeout context) so 100s of concurrent callers do not produce the
	// pile-up that Acquire(timeout) did.
	if o.connSem.TryAcquire(1) {
		conn, err := o.createConnection(to)
		if err != nil {
			// Release the sem unit we just took; otherwise it would be
			// permanently held by a non-existent connection and the pool
			// effective capacity would shrink on every transient error.
			o.connSem.Release(1)
			outcome = "create_err"
			return nil, err
		}
		outcome = "new_conn"
		return conn, nil
	}

	// Slow path: pool empty AND sem at capacity (every conn we are allowed
	// to hold is in flight). Block until one returns to the pool. No 1s
	// timeout retry — relying on the caller's ctx covers global cancellation
	// while normal completion arrives within milliseconds at steady state.
	select {
	case connection := <-pool:
		outcome = "pool_wait"
		return connection, nil
	case <-ctx.Done():
		outcome = "ctx_done"
		return nil, ctx.Err()
	}
}

// createConnection performs the three-step gRPC stream setup for a single
// orderer. Extracted so getConnection's control flow stays linear and so the
// error paths can release the semaphore in one place.
func (o *BFTBroadcaster) createConnection(to *grpc.ConnectionConfig) (*Connection, error) {
	client, err := o.ClientFactory.NewOrdererClient(*to)
	if err != nil {
		return nil, errors.Wrapf(err, "failed creating orderer client for %s", to.Address)
	}

	oClient, err := client.OrdererClient()
	if err != nil {
		rpcStatus, _ := status.FromError(err)
		return nil, errors.Wrapf(err, "failed to new a broadcast, rpcStatus=%+v", rpcStatus)
	}

	// The stream uses context.Background() because it is shared across many
	// broadcasts; tying it to a per-broadcast ctx would break stream reuse.
	stream, err := oClient.Broadcast(context.Background())
	if err != nil {
		client.Close()
		return nil, errors.Wrapf(err, "failed creating orderer stream for %s", to.Address)
	}

	return &Connection{
		Stream: stream,
		Client: client,
	}, nil
}

func (o *BFTBroadcaster) discardConnection(connection *Connection) {
	if connection != nil {
		o.connSem.Release(1)
		if connection.Stream != nil {
			if err := connection.Stream.CloseSend(); err != nil {
				logger.Warnf("failed to close connection to ordering [%s]", err)
			}
		}
		if connection.Client != nil {
			connection.Client.Close()
		}
	}
}

func (o *BFTBroadcaster) releaseConnection(connection *Connection, to *grpc.ConnectionConfig) {
	pool := o.connectionPool(to.Address)
	select {
	case pool <- connection:
		return
	default:
		// if there is not enough space in the channel, then discard the connection
		o.discardConnection(connection)
	}
}

func (o *BFTBroadcaster) connectionPool(id string) chan *Connection {
	o.connectionsLock.RLock()
	connections, ok := o.connections[id]
	o.connectionsLock.RUnlock()
	if !ok {
		o.connectionsLock.Lock()
		connections, ok = o.connections[id]
		if !ok {
			connections = make(chan *Connection, o.poolSize)
			o.connections[id] = connections
		}
		o.connectionsLock.Unlock()
	}

	return connections
}

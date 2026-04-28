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
	"google.golang.org/grpc/status"

	"github.com/hyperledger-labs/fabric-smart-client/pkg/utils/errors"
	"github.com/hyperledger-labs/fabric-smart-client/platform/fabric/core/generic/metrics"
	"github.com/hyperledger-labs/fabric-smart-client/platform/fabric/driver"
	"github.com/hyperledger-labs/fabric-smart-client/platform/view/services/grpc"
)

// BFTBroadcaster fans broadcast envelopes out to N≥4 orderers and returns as
// soon as f+1 acks are observed. Connection state is partitioned per orderer
// so neither connection caching nor connection-creation backpressure
// serialize through a single mutex when many goroutines call Broadcast
// concurrently.
type BFTBroadcaster struct {
	ConfigService driver.ConfigService
	ClientFactory Services

	metrics  *metrics.Metrics
	poolSize int

	stateLock sync.RWMutex
	state     map[string]*bftOrdererState
}

// bftOrdererState holds the per-orderer pool and creation semaphore. Both
// are lazily allocated on first reference and never destroyed.
type bftOrdererState struct {
	// pool caches idle connections for reuse. Capacity = poolSize.
	pool chan *Connection
	// sem caps the number of live connections (in pool + in flight) for
	// this orderer. Implemented as a buffered channel, not
	// golang.org/x/sync/semaphore.Weighted: runtime channel ops do not
	// contend on a shared sync.Mutex on the fast path, which removes the
	// throughput cliff observed when ~hundreds of goroutines call Acquire
	// concurrently.
	sem chan struct{}
}

func NewBFTBroadcaster(configService driver.ConfigService, cf Services, metrics *metrics.Metrics) *BFTBroadcaster {
	return &BFTBroadcaster{
		ConfigService: configService,
		ClientFactory: cf,
		metrics:       metrics,
		poolSize:      configService.OrdererConnectionPoolSize(),
		state:         map[string]*bftOrdererState{},
	}
}

func (o *BFTBroadcaster) Broadcast(ctx context.Context, env *common2.Envelope) error {
	logger.DebugfContext(ctx, "Start BFT Broadcast")
	defer logger.DebugfContext(ctx, "End BFT Broadcast")
	// send the envelope for ordering
	retries := o.ConfigService.BroadcastNumRetries()
	retryInterval := o.ConfigService.BroadcastRetryInterval()
	orderers := o.ConfigService.Orderers()
	if len(orderers) < 4 {
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

		var lock sync.Mutex
		counter := 0
		var errs []error
		var wg sync.WaitGroup
		wg.Add(n)

		// Bound this iteration with a cancelable context so we can stop the
		// in-flight Recv() calls on the remaining N - threshold orderers as
		// soon as f+1 acks land. Without this, broadcast latency is capped
		// by max(orderer RTT), so a single slow orderer pins p99 to its RTT.
		iterCtx, cancel := context.WithCancel(ctx)
		thresholdMet := make(chan struct{})
		var thresholdOnce sync.Once

		for _, orderer := range orderers {
			go func(orderer *grpc.ConnectionConfig) {
				defer wg.Done()

				logger.DebugfContext(iterCtx, "get connection to [%s]", orderer.Address)
				connection, err := o.getConnection(iterCtx, orderer)
				if err != nil {
					lock.Lock()
					errs = append(errs, errors.Wrapf(err, "failed connecting to [%v]", orderer.Address))
					lock.Unlock()
					logger.WarnfContext(iterCtx, "failed to get connection to orderer [%s]", orderer.Address, err)
					return
				}

				logger.DebugfContext(iterCtx, "broadcast to [%s]", orderer.Address)
				if err := connection.Send(env); err != nil {
					logger.ErrorfContext(iterCtx, "failed to broadcast to [%s]: %s", orderer.Address, err.Error())
					o.discardConnection(connection, orderer.Address)
					return
				}
				ack, err := connection.Recv()
				if err != nil {
					logger.ErrorfContext(iterCtx, "failed to get status after broadcast to [%s]: %s", orderer.Address, err.Error())
					o.discardConnection(connection, orderer.Address)
					return
				}

				switch ack.GetStatus() {
				case common2.Status_SUCCESS:
					o.releaseConnection(connection, orderer)
					lock.Lock()
					counter++
					if counter >= threshold {
						thresholdOnce.Do(func() { close(thresholdMet) })
					}
					lock.Unlock()
				default:
					logger.ErrorfContext(iterCtx, "failed to get status after broadcast to [%s]: %s", orderer.Address, common2.Status_name[int32(ack.GetStatus())])
					o.discardConnection(connection, orderer.Address)
					lock.Lock()
					errs = append(errs, fmt.Errorf("failed to get status after broadcast to [%s]: %s", orderer.Address, common2.Status_name[int32(ack.GetStatus())]))
					lock.Unlock()
				}
			}(orderer)
		}

		// Wait for either threshold success or all goroutines done.
		allDone := make(chan struct{})
		go func() { wg.Wait(); close(allDone) }()
		select {
		case <-thresholdMet:
			// Enough acks; the remaining sub-goroutines run in the
			// background and will hit iterCtx cancellation, then either
			// release or discard their connections themselves.
			cancel()
			return nil
		case <-allDone:
		}
		cancel()

		// fail
		lock.Lock()
		currentCounter := counter
		currentErrs := append([]error(nil), errs...)
		lock.Unlock()
		logger.WarnfContext(ctx, "failed to broadcast, got [%d of %d] success and errs [%v], retry after a delay", currentCounter, threshold, currentErrs)
	}

	return errors.Errorf("failed to send transaction to the orderering service")
}

// getConnection returns a connection to the given orderer, either reusing
// one from the pool or creating a new one. If the orderer has reached its
// concurrent-connection cap and no connections are available for reuse,
// the call blocks until either a connection is released back to the pool,
// a creation slot frees up, or ctx is canceled.
//
// Unlike a sync.Mutex-backed semaphore, the wait does not serialize through
// a shared application-level lock — callers either succeed immediately on
// the fast path or park in the runtime channel wait queue.
func (o *BFTBroadcaster) getConnection(ctx context.Context, to *grpc.ConnectionConfig) (*Connection, error) {
	state := o.getOrdererState(to.Address)

	// Fast path: reuse a cached connection if one is available right now.
	select {
	case connection := <-state.pool:
		return connection, nil
	default:
	}

	// Slow path: wait for either a returned connection, a free creation
	// slot, or context cancellation. Go's select picks a ready case
	// uniformly at random, so under burst we may transiently dial above
	// the steady-state population; surplus connections will be discarded
	// on release when the pool is full.
	select {
	case connection := <-state.pool:
		return connection, nil
	case state.sem <- struct{}{}:
		connection, err := o.dialNew(to)
		if err != nil {
			<-state.sem
			return nil, err
		}
		return connection, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// dialNew opens a fresh broadcast stream to the given orderer. The caller
// must already hold a creation slot in state.sem; the slot is the caller's
// to release on error.
func (o *BFTBroadcaster) dialNew(to *grpc.ConnectionConfig) (*Connection, error) {
	client, err := o.ClientFactory.NewOrdererClient(*to)
	if err != nil {
		return nil, errors.Wrapf(err, "failed creating orderer client for %s", to.Address)
	}

	oClient, err := client.OrdererClient()
	if err != nil {
		client.Close()
		rpcStatus, _ := status.FromError(err)
		return nil, errors.Wrapf(err, "failed to new a broadcast, rpcStatus=%+v", rpcStatus)
	}

	// Get the broadcast stream to receive a reply of Acknowledgement for each common.Envelope in order, indicating success or type of failure.
	// Notice that this stream is shared, therefore its context must be something different from the context of the current broadcast request
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

// discardConnection closes the connection and releases its creation slot
// back to the per-orderer semaphore. addr identifies which orderer the
// connection was dialed to; it must match the address used to obtain the
// connection so the right sem is decremented.
func (o *BFTBroadcaster) discardConnection(connection *Connection, addr string) {
	if connection == nil {
		return
	}
	state := o.getOrdererState(addr)
	<-state.sem
	if connection.Stream != nil {
		if err := connection.Stream.CloseSend(); err != nil {
			logger.Warnf("failed to close connection to ordering [%s]", err)
		}
	}
	if connection.Client != nil {
		connection.Client.Close()
	}
}

// releaseConnection returns a healthy connection to the per-orderer pool
// for reuse, or discards it if the pool is full.
func (o *BFTBroadcaster) releaseConnection(connection *Connection, to *grpc.ConnectionConfig) {
	state := o.getOrdererState(to.Address)
	select {
	case state.pool <- connection:
		return
	default:
		// pool full; close this one
		o.discardConnection(connection, to.Address)
	}
}

// getOrdererState lazily allocates the per-orderer pool and semaphore.
func (o *BFTBroadcaster) getOrdererState(addr string) *bftOrdererState {
	o.stateLock.RLock()
	state, ok := o.state[addr]
	o.stateLock.RUnlock()
	if ok {
		return state
	}

	o.stateLock.Lock()
	defer o.stateLock.Unlock()
	if state, ok := o.state[addr]; ok {
		return state
	}
	state = &bftOrdererState{
		pool: make(chan *Connection, o.poolSize),
		sem:  make(chan struct{}, o.poolSize),
	}
	o.state[addr] = state
	return state
}

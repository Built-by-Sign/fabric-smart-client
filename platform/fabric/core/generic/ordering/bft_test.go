/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package ordering

import (
	"context"
	"testing"
	"time"

	common "github.com/hyperledger/fabric-protos-go-apiv2/common"
	ab "github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/stretchr/testify/require"
	ggrpc "google.golang.org/grpc"

	"github.com/hyperledger-labs/fabric-smart-client/platform/fabric/driver"
	"github.com/hyperledger-labs/fabric-smart-client/platform/view/services/grpc"
)

// fakeConfig overrides only the ConfigService method NewBFTBroadcaster reads.
type fakeConfig struct {
	driver.ConfigService
	poolSize int
}

func (c fakeConfig) OrdererConnectionPoolSize() int { return c.poolSize }

// fakeServices hands out orderer clients whose stream creation is instant, so
// tests exercise the acquire/pool machinery without real network I/O.
type fakeServices struct{}

func (fakeServices) NewOrdererClient(grpc.ConnectionConfig) (Client, error) { return fakeClient{}, nil }

type fakeClient struct{ Client }

func (fakeClient) OrdererClient() (ab.AtomicBroadcastClient, error) { return fakeAB{}, nil }
func (fakeClient) Close()                                           {}

type fakeAB struct{}

func (fakeAB) Broadcast(context.Context, ...ggrpc.CallOption) (ggrpc.BidiStreamingClient[common.Envelope, ab.BroadcastResponse], error) {
	return fakeStream{}, nil
}

func (fakeAB) Deliver(context.Context, ...ggrpc.CallOption) (ggrpc.BidiStreamingClient[common.Envelope, ab.DeliverResponse], error) {
	return nil, nil
}

type fakeStream struct {
	ggrpc.BidiStreamingClient[common.Envelope, ab.BroadcastResponse]
}

func (fakeStream) Send(*common.Envelope) error { return nil }
func (fakeStream) Recv() (*ab.BroadcastResponse, error) {
	return &ab.BroadcastResponse{Status: common.Status_SUCCESS}, nil
}
func (fakeStream) CloseSend() error { return nil }

// getConnection must return promptly when the caller's context is cancelled
// while no connection is obtainable (pool empty, every slot held). The acquire
// path blocks on a select that includes ctx.Done(); a regression dropping that
// case would busy-spin and miss the deadline.
func TestBFTBroadcaster_GetConnectionHonorsContextCancellation(t *testing.T) {
	t.Parallel()

	const poolSize = 4
	b := NewBFTBroadcaster(fakeConfig{poolSize: poolSize}, fakeServices{}, nil)
	to := &grpc.ConnectionConfig{Address: "orderer-0"}

	// Exhaust the target: create poolSize connections and keep them checked
	// out, so the next getConnection can neither reuse nor create.
	for i := 0; i < poolSize; i++ {
		c, err := b.getConnection(context.Background(), to)
		require.NoError(t, err)
		require.NotNil(t, c)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan error, 1)
	go func() {
		_, err := b.getConnection(ctx, to)
		done <- err
	}()

	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("getConnection ignored a cancelled context (busy-spin)")
	}
}

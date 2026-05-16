/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package ordering

import (
	"sync"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	ab "github.com/hyperledger/fabric-protos-go-apiv2/orderer"

	"github.com/hyperledger-labs/fabric-smart-client/platform/fabric/core/generic/services"
	"github.com/hyperledger-labs/fabric-smart-client/platform/view/services/grpc"
)

type Connection struct {
	lock sync.Mutex
	// Address is the orderer this connection was created for. Used by
	// BFTBroadcaster.discardConnection to release the right per-orderer
	// semaphore (see obsidian "CBDC 压测优化迭代 2026-05-15" §4.5).
	Address string
	Stream  Broadcast
	Client  Client
}

func (c *Connection) Send(m *common.Envelope) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	return c.Stream.Send(m)
}

func (c *Connection) Recv() (*ab.BroadcastResponse, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	return c.Stream.Recv()
}

type Client = services.OrdererClient

type Services interface {
	NewOrdererClient(cc grpc.ConnectionConfig) (Client, error)
}

// Broadcast defines the interface that abstracts grpc calls to broadcast transactions to orderer
type Broadcast interface {
	Send(m *common.Envelope) error
	Recv() (*ab.BroadcastResponse, error)
	CloseSend() error
}

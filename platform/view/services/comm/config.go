/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package comm

const (
	// DefaultIncomingMessagesBufferSize is the default buffer size for the incoming messages channel
	DefaultIncomingMessagesBufferSize = 1024
	// DefaultStreamReaderBufferSize is the default buffer size for stream readers
	DefaultStreamReaderBufferSize = 4096
	// DefaultMaxStreamsPerPeer is the default fan-out cap of outgoing streams per
	// peer in sendWithCachedStreams before falling back to a blocking send. libp2p
	// hashes streams by peer ID only, so without fan-out every send to a peer
	// serializes on one stream's write lock + yamux window; the cap stops a
	// sustained burst against a slow peer from opening unbounded streams.
	DefaultMaxStreamsPerPeer = 8
)

type configService interface {
	GetInt(key string) int
	IsSet(key string) bool
}

type config struct {
	incomingMessagesBufferSize int
	streamReaderBufferSize     int
	dispatcherWorkers          int
	maxStreamsPerPeer          int
}

func NewConfig(cs configService) *config {
	incomingMessagesBufferSize := DefaultIncomingMessagesBufferSize
	if cs.IsSet("fsc.p2p.incomingMessagesBufferSize") {
		incomingMessagesBufferSize = cs.GetInt("fsc.p2p.incomingMessagesBufferSize")
	}

	streamReaderBufferSize := DefaultStreamReaderBufferSize
	if cs.IsSet("fsc.p2p.streamReaderBufferSize") {
		streamReaderBufferSize = cs.GetInt("fsc.p2p.streamReaderBufferSize")
	}

	dispatcherWorkers := DefaultDispatcherWorkers
	if cs.IsSet("fsc.p2p.dispatcherWorkers") {
		dispatcherWorkers = cs.GetInt("fsc.p2p.dispatcherWorkers")
	}

	maxStreamsPerPeer := DefaultMaxStreamsPerPeer
	if cs.IsSet("fsc.p2p.maxStreamsPerPeer") {
		maxStreamsPerPeer = cs.GetInt("fsc.p2p.maxStreamsPerPeer")
	}

	return &config{
		incomingMessagesBufferSize: incomingMessagesBufferSize,
		streamReaderBufferSize:     streamReaderBufferSize,
		dispatcherWorkers:          dispatcherWorkers,
		maxStreamsPerPeer:          maxStreamsPerPeer,
	}
}

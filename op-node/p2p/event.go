package p2p

import (
	"context"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/ethereum/go-ethereum/log"

	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum-optimism/optimism/op-service/event"
)

type ReceivedBlockEvent struct {
	From     peer.ID
	Envelope *eth.ExecutionPayloadEnvelope
}

func (ev ReceivedBlockEvent) String() string {
	return "received-block-event"
}

type BlockReceiverMetrics interface {
	RecordReceivedUnsafePayload(payload *eth.ExecutionPayloadEnvelope)
}

// BlockReceiver can be plugged into the P2P gossip stack,
// to receive payloads as ReceivedBlockEvent events.
type BlockReceiver struct {
	log     log.Logger
	emitter event.Emitter
	metrics BlockReceiverMetrics
}

var _ GossipIn = (*BlockReceiver)(nil)

func NewBlockReceiver(log log.Logger, em event.Emitter, metrics BlockReceiverMetrics) *BlockReceiver {
	return &BlockReceiver{
		log:     log,
		emitter: em,
		metrics: metrics,
	}
}

func (g *BlockReceiver) OnUnsafeL2Payload(ctx context.Context, from peer.ID, msg *eth.ExecutionPayloadEnvelope) error {
	g.log.Debug("Received signed execution payload from p2p",
		"id", msg.ExecutionPayload.ID(),
		"peer", from, "txs", len(msg.ExecutionPayload.Transactions))
	g.metrics.RecordReceivedUnsafePayload(msg)
	g.emitter.Emit(ctx, ReceivedBlockEvent{From: from, Envelope: msg})
	return nil
}

func (g *BlockReceiver) OnSafeL2Payload(ctx context.Context, from peer.ID, msg *eth.ExecutionPayloadEnvelope) error {
	g.log.Debug("Received signed safe head execution payload from p2p",
		"id", msg.ExecutionPayload.ID(),
		"peer", from, "txs", len(msg.ExecutionPayload.Transactions))
	// For now, we can reuse the same metrics as unsafe payloads
	// In the future, we might want separate safe head metrics
	g.metrics.RecordReceivedUnsafePayload(msg)
	g.emitter.Emit(ctx, ReceivedSafeHeadEvent{From: from, Envelope: msg})
	return nil
}

type ReceivedSafeHeadEvent struct {
	From     peer.ID
	Envelope *eth.ExecutionPayloadEnvelope
}

func (ev ReceivedSafeHeadEvent) String() string {
	return "received-safe-head-event"
}

type SafeHeadReceiverMetrics interface {
	RecordReceivedSafePayload(payload *eth.ExecutionPayloadEnvelope)
}

// SafeHeadReceiver can be plugged into the P2P gossip stack,
// to receive safe head payloads as ReceivedSafeHeadEvent events.
type SafeHeadReceiver struct {
	log     log.Logger
	emitter event.Emitter
	metrics SafeHeadReceiverMetrics
}

var _ SafeHeadGossipIn = (*SafeHeadReceiver)(nil)

func NewSafeHeadReceiver(log log.Logger, em event.Emitter, metrics SafeHeadReceiverMetrics) *SafeHeadReceiver {
	return &SafeHeadReceiver{
		log:     log,
		emitter: em,
		metrics: metrics,
	}
}

func (r *SafeHeadReceiver) OnSafeL2Payload(ctx context.Context, from peer.ID, msg *eth.ExecutionPayloadEnvelope) error {
	r.log.Debug("Received signed safe head execution payload from p2p",
		"id", msg.ExecutionPayload.ID(),
		"peer", from, "txs", len(msg.ExecutionPayload.Transactions))
	r.metrics.RecordReceivedSafePayload(msg)
	r.emitter.Emit(ctx, ReceivedSafeHeadEvent{From: from, Envelope: msg})
	return nil
}

// GossipSafeHeadEvent is emitted when a prover node wants to gossip a safe head
type GossipSafeHeadEvent struct {
	Envelope *eth.ExecutionPayloadEnvelope
}

func (ev GossipSafeHeadEvent) String() string {
	return "gossip-safe-head-event"
}

// SafeHeadGossipPublisher listens to GossipSafeHeadEvent and publishes safe heads to P2P network
type SafeHeadGossipPublisher struct {
	log     log.Logger
	network Network
	emitter event.Emitter
}

type Network interface {
	SignAndPublishSafeHead(ctx context.Context, envelope *eth.ExecutionPayloadEnvelope) error
}

var _ event.Deriver = (*SafeHeadGossipPublisher)(nil)

func NewSafeHeadGossipPublisher(log log.Logger, network Network) *SafeHeadGossipPublisher {
	return &SafeHeadGossipPublisher{
		log:     log,
		network: network,
	}
}

func (p *SafeHeadGossipPublisher) AttachEmitter(em event.Emitter) {
	p.emitter = em
}

func (p *SafeHeadGossipPublisher) OnEvent(ctx context.Context, ev event.Event) bool {
	switch x := ev.(type) {
	case GossipSafeHeadEvent:
		if err := p.network.SignAndPublishSafeHead(ctx, x.Envelope); err != nil {
			p.log.Warn("Failed to publish safe head to P2P",
				"id", x.Envelope.ExecutionPayload.ID(),
				"hash", x.Envelope.ExecutionPayload.BlockHash,
				"err", err)
		} else {
			p.log.Debug("Published safe head to P2P",
				"id", x.Envelope.ExecutionPayload.ID(),
				"hash", x.Envelope.ExecutionPayload.BlockHash)
		}
		return true
	default:
		return false
	}
}

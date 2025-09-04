package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/ethereum-optimism/optimism/op-node/rollup"
	"github.com/ethereum-optimism/optimism/op-service/eth"
)

type PayloadProcessEvent struct {
	// if payload should be promoted to (local) safe (must also be pending safe, see DerivedFrom)
	Concluding bool
	// payload is promoted to pending-safe if non-zero
	DerivedFrom  eth.L1BlockRef
	BuildStarted time.Time

	Envelope *eth.ExecutionPayloadEnvelope
	Ref      eth.L2BlockRef
}

func (ev PayloadProcessEvent) String() string {
	return "payload-process"
}

func (eq *EngDeriver) onPayloadProcess(ctx context.Context, ev PayloadProcessEvent) {
	rpcCtx, cancel := context.WithTimeout(eq.ctx, payloadProcessTimeout)
	defer cancel()

	// Optional SV2 denylist pre-insert check
	if baseURL := os.Getenv("SV2_AUTHORIZATION_URL"); baseURL != "" && ev.Envelope != nil && ev.Envelope.ExecutionPayload != nil {
		if payloadID, ok := ev.Envelope.CheckBlockHash(); ok {
			chainID := eq.cfg.L2ChainID.Uint64()
			url := fmt.Sprintf("%s/denylist/v1/check?chainId=%d&id=%s", baseURL, chainID, payloadID.Hex())
			reqCtx, reqCancel := context.WithTimeout(eq.ctx, 500*time.Millisecond)
			req, _ := http.NewRequestWithContext(reqCtx, http.MethodGet, url, nil)
			resp, err := http.DefaultClient.Do(req)
			if err == nil && resp != nil && resp.Body != nil {
				var out struct {
					Denylisted bool `json:"denylisted"`
				}
				_ = json.NewDecoder(resp.Body).Decode(&out)
				resp.Body.Close()
				reqCancel()
				if out.Denylisted {
					if ev.DerivedFrom != (eth.L1BlockRef{}) && eq.cfg.IsHolocene(ev.DerivedFrom.Time) {
						eq.emitDepositsOnlyPayloadAttributesRequest(ctx, ev.Ref.ParentID(), ev.DerivedFrom)
						return
					}
					eq.emitter.Emit(ctx, PayloadInvalidEvent{
						Envelope: ev.Envelope,
						Err:      fmt.Errorf("sv2 denylisted payload %s", payloadID.Hex()),
					})
					return
				}
			} else {
				if resp != nil && resp.Body != nil {
					resp.Body.Close()
				}
				reqCancel()
			}
		}
	}

	insertStart := time.Now()
	status, err := eq.ec.engine.NewPayload(rpcCtx,
		ev.Envelope.ExecutionPayload, ev.Envelope.ParentBeaconBlockRoot)
	if err != nil {
		eq.emitter.Emit(ctx, rollup.EngineTemporaryErrorEvent{
			Err: fmt.Errorf("failed to insert execution payload: %w", err),
		})
		return
	}
	switch status.Status {
	case eth.ExecutionInvalid, eth.ExecutionInvalidBlockHash:
		// Depending on execution engine, not all block-validity checks run immediately on build-start
		// at the time of the forkchoiceUpdated engine-API call, nor during getPayload.
		if ev.DerivedFrom != (eth.L1BlockRef{}) && eq.cfg.IsHolocene(ev.DerivedFrom.Time) {
			eq.emitDepositsOnlyPayloadAttributesRequest(ctx, ev.Ref.ParentID(), ev.DerivedFrom)
			return
		}

		eq.emitter.Emit(ctx, PayloadInvalidEvent{
			Envelope: ev.Envelope,
			Err:      eth.NewPayloadErr(ev.Envelope.ExecutionPayload, status),
		})
		return
	case eth.ExecutionValid:
		eq.emitter.Emit(ctx, PayloadSuccessEvent{
			Concluding:    ev.Concluding,
			DerivedFrom:   ev.DerivedFrom,
			BuildStarted:  ev.BuildStarted,
			InsertStarted: insertStart,
			Envelope:      ev.Envelope,
			Ref:           ev.Ref,
		})
		return
	default:
		eq.emitter.Emit(ctx, rollup.EngineTemporaryErrorEvent{
			Err: eth.NewPayloadErr(ev.Envelope.ExecutionPayload, status),
		})
		return
	}
}

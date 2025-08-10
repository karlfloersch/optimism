package supervisor

import (
	"context"
	"fmt"
	"strconv"

	opclient "github.com/ethereum-optimism/optimism/op-service/client"
)

// rollbackEL is the high-level EL rollback helper used by the supervisor.
// TODO: Replace the debug_setHead-based implementation with an Engine API
// forkchoice update (engine_forkchoiceUpdated) against the authenticated EL RPC
// to support ELs like reth without changing finalized.
func rollbackEL(ctx context.Context, l2UserRPC string, backN uint64) error {
	return rollbackELWithDebugSetHead(ctx, l2UserRPC, backN)
}

// rollbackELWithDebugSetHead rolls the EL back by N blocks using debug_setHead via the user RPC.
func rollbackELWithDebugSetHead(ctx context.Context, l2UserRPC string, backN uint64) error {
	cli, err := opclient.NewRPC(ctx, nil, l2UserRPC)
	if err != nil {
		return fmt.Errorf("dial l2 user rpc: %w", err)
	}
	defer cli.Close()

	type header struct {
		Number     string `json:"number"`
		ParentHash string `json:"parentHash"`
		Hash       string `json:"hash"`
	}
	var h header
	// get latest block header
	if err := cli.CallContext(ctx, &h, "eth_getBlockByNumber", "latest", false); err != nil {
		return fmt.Errorf("get latest: %w", err)
	}
	// parse latest number (hex string like 0x...)
	numStr := h.Number
	if len(numStr) < 3 || numStr[:2] != "0x" {
		return fmt.Errorf("unexpected number format: %s", numStr)
	}
	latestNum, err := strconv.ParseUint(numStr[2:], 16, 64)
	if err != nil {
		return fmt.Errorf("parse latest number: %w", err)
	}
	// roll back head in a single call to the exact target height
	if backN > latestNum {
		backN = latestNum
	}
	target := latestNum - backN
	var ok bool
	if err := cli.CallContext(ctx, &ok, "debug_setHead", fmt.Sprintf("0x%x", target)); err != nil {
		return fmt.Errorf("debug_setHead: %w", err)
	}
	return nil
}

package super

import (
	"context"
	"fmt"

	opclient "github.com/ethereum-optimism/optimism/op-service/client"
)

// rollbackEL is the high-level EL rollback helper used by the super.
// TODO: Replace the debug_setHead-based implementation with an Engine API
// forkchoice update (engine_forkchoiceUpdated) against the authenticated EL RPC
// to support ELs like reth without changing finalized.
func rollbackEL(ctx context.Context, l2UserRPC string, target uint64) error {
	return rollbackELWithDebugSetHead(ctx, l2UserRPC, target)
}

// rollbackELWithDebugSetHead rolls the EL back by N blocks using debug_setHead via the user RPC.
func rollbackELWithDebugSetHead(ctx context.Context, l2UserRPC string, target uint64) error {
	cli, err := opclient.NewRPC(ctx, nil, l2UserRPC)
	if err != nil {
		return fmt.Errorf("dial l2 user rpc: %w", err)
	}
	defer cli.Close()

	var ok bool
	if err := cli.CallContext(ctx, &ok, "debug_setHead", fmt.Sprintf("0x%x", target)); err != nil {
		return fmt.Errorf("debug_setHead: %w", err)
	}
	return nil
}

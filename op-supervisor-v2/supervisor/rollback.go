package supervisor

import (
	"context"
	"fmt"

	opclient "github.com/ethereum-optimism/optimism/op-service/client"
)

// rollbackELByDebugSetHead rolls the EL back by N blocks using debug_setHead via the user RPC.
// It fetches the current head, then sets head to parent repeatedly N times.
func rollbackELByDebugSetHead(ctx context.Context, l2UserRPC string, backN uint64) error {
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
	parent := h.ParentHash
	for i := uint64(0); i < backN; i++ {
		// set head to parent
		var ok bool
		if err := cli.CallContext(ctx, &ok, "debug_setHead", parent); err != nil {
			return fmt.Errorf("debug_setHead: %w", err)
		}
		// fetch new head to get next parent
		if err := cli.CallContext(ctx, &h, "eth_getBlockByNumber", "latest", false); err != nil {
			return fmt.Errorf("get latest after setHead: %w", err)
		}
		parent = h.ParentHash
	}
	return nil
}

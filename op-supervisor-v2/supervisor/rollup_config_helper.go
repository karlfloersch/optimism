package supervisor

import (
	"context"

	"github.com/ethereum-optimism/optimism/op-node/rollup"
	opclient "github.com/ethereum-optimism/optimism/op-service/client"
	"github.com/ethereum-optimism/optimism/op-service/sources"
)

// RollupConfigFromRPC fetches the rollup config from an op-node RPC client.
func RollupConfigFromRPC(ctx context.Context, rpc opclient.RPC) (*rollup.Config, error) {
	roll := sources.NewRollupClient(rpc)
	return roll.RollupConfig(ctx)
}

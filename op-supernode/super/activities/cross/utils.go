package cross

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/ethereum-optimism/optimism/op-node/rollup"
	opclient "github.com/ethereum-optimism/optimism/op-service/client"
	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum-optimism/optimism/op-service/sources"
)

// defaultScopeLabel returns the default L1 scope label
// it can be overridden via env SV2_L1_SCOPE
func defaultScopeLabel() eth.BlockLabel {
	switch strings.ToLower(os.Getenv("SV2_L1_SCOPE")) {
	case "unsafe":
		return eth.Unsafe
	case "safe":
		return eth.Safe
	case "finalized":
		return eth.Finalized
	}
	return eth.Safe
}

// blockAtTimestampFromConfig returns the L2 block number whose timestamp is <= ts, using rollup config.
// It clamps to genesis and accounts for non-zero genesis block numbers.
func blockAtTimestampFromConfig(rcfg *rollup.Config, ts uint64) (uint64, error) {
	if rcfg == nil {
		return 0, fmt.Errorf("nil rollup config")
	}
	if rcfg.BlockTime == 0 {
		return 0, fmt.Errorf("blockTime must be a positive integer")
	}
	genesisTime := rcfg.Genesis.L2Time
	genesisNum := rcfg.Genesis.L2.Number
	if ts <= genesisTime {
		return genesisNum, nil
	}
	return genesisNum + ((ts - genesisTime) / rcfg.BlockTime), nil
}

// EnsureL1Client lazily initializes the L1 client using the given RPC URL.
func (s *CrossService) EnsureL1Client(ctx context.Context, l1Cli opclient.RPC, l1 *sources.L1Client, l1RPC string, rcfg *rollup.Config) (opclient.RPC, *sources.L1Client) {
	if l1 != nil {
		return l1Cli, l1
	}
	if l1Cli == nil {
		if c, e := opclient.NewRPC(ctx, s.log, l1RPC); e == nil {
			l1Cli = c
		}
	}
	if l1Cli != nil {
		if l1Client, e := sources.NewL1Client(l1Cli, s.log, nil, sources.L1ClientDefaultConfig(rcfg, true, sources.RPCKindStandard)); e == nil {
			l1 = l1Client
		}
	}
	return l1Cli, l1
}

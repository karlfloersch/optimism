package flags

import (
	"fmt"
	"strings"

	"github.com/urfave/cli/v2"

	opservice "github.com/ethereum-optimism/optimism/op-service"
	oplog "github.com/ethereum-optimism/optimism/op-service/log"
	opmetrics "github.com/ethereum-optimism/optimism/op-service/metrics"
	"github.com/ethereum-optimism/optimism/op-service/oppprof"
	oprpc "github.com/ethereum-optimism/optimism/op-service/rpc"
)

const EnvVarPrefix = "OP_INTEROP_FILTER"

func prefixEnvVars(name string) []string {
	return opservice.PrefixEnvVar(EnvVarPrefix, name)
}

var (
	L2RPCsFlag = &cli.StringFlag{
		Name:     "l2-rpcs",
		Usage:    "Comma-separated list of L2 RPC endpoints in format chainID:rpcURL (e.g., 10:http://op-mainnet:8545,8453:http://base:8545)",
		EnvVars:  prefixEnvVars("L2_RPCS"),
		Required: true,
	}
	DataDirFlag = &cli.StringFlag{
		Name:    "data-dir",
		Usage:   "Directory for LogsDB storage. If empty, uses in-memory storage",
		EnvVars: prefixEnvVars("DATA_DIR"),
		Value:   "",
	}
	BackfillDurationFlag = &cli.StringFlag{
		Name:    "backfill-duration",
		Usage:   "Duration to backfill on startup (e.g., 24h, 30m, 1h30m)",
		EnvVars: prefixEnvVars("BACKFILL_DURATION"),
		Value:   "24h",
	}
)

var requiredFlags = []cli.Flag{
	L2RPCsFlag,
}

var optionalFlags = []cli.Flag{
	DataDirFlag,
	BackfillDurationFlag,
}

func init() {
	optionalFlags = append(optionalFlags, oprpc.CLIFlags(EnvVarPrefix)...)
	optionalFlags = append(optionalFlags, oplog.CLIFlags(EnvVarPrefix)...)
	optionalFlags = append(optionalFlags, opmetrics.CLIFlags(EnvVarPrefix)...)
	optionalFlags = append(optionalFlags, oppprof.CLIFlags(EnvVarPrefix)...)

	Flags = append(requiredFlags, optionalFlags...)
}

var Flags []cli.Flag

func CheckRequired(ctx *cli.Context) error {
	for _, f := range requiredFlags {
		name := f.Names()[0]
		if !ctx.IsSet(name) {
			return fmt.Errorf("flag %s is required", name)
		}
	}
	// Validate L2RPCs format
	l2rpcs := ctx.String(L2RPCsFlag.Name)
	if _, err := ParseL2RPCs(l2rpcs); err != nil {
		return fmt.Errorf("invalid --%s: %w", L2RPCsFlag.Name, err)
	}
	return nil
}

// L2RPC represents a chain ID to RPC URL mapping
type L2RPC struct {
	ChainID uint64
	RPCURL  string
}

// ParseL2RPCs parses the l2-rpcs flag format: "chainID:rpcURL,chainID:rpcURL,..."
func ParseL2RPCs(s string) ([]L2RPC, error) {
	if s == "" {
		return nil, fmt.Errorf("empty l2-rpcs")
	}
	var result []L2RPC
	pairs := strings.Split(s, ",")
	for _, pair := range pairs {
		parts := strings.SplitN(strings.TrimSpace(pair), ":", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid format %q, expected chainID:rpcURL", pair)
		}
		var chainID uint64
		if _, err := fmt.Sscanf(parts[0], "%d", &chainID); err != nil {
			return nil, fmt.Errorf("invalid chain ID %q: %w", parts[0], err)
		}
		rpcURL := parts[1]
		if rpcURL == "" {
			return nil, fmt.Errorf("empty RPC URL for chain %d", chainID)
		}
		result = append(result, L2RPC{ChainID: chainID, RPCURL: rpcURL})
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("no L2 RPCs configured")
	}
	return result, nil
}

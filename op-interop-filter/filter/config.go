package filter

import (
	"errors"

	"github.com/urfave/cli/v2"

	"github.com/ethereum-optimism/optimism/op-interop-filter/flags"
	oplog "github.com/ethereum-optimism/optimism/op-service/log"
	opmetrics "github.com/ethereum-optimism/optimism/op-service/metrics"
	"github.com/ethereum-optimism/optimism/op-service/oppprof"
	oprpc "github.com/ethereum-optimism/optimism/op-service/rpc"
)

type Config struct {
	L2RPCs        []flags.L2RPC
	DataDir       string
	BackfillHours uint64
	Version       string

	LogConfig     oplog.CLIConfig
	MetricsConfig opmetrics.CLIConfig
	PprofConfig   oppprof.CLIConfig
	RPC           oprpc.CLIConfig
}

func (c *Config) Check() error {
	var result error
	if len(c.L2RPCs) == 0 {
		result = errors.Join(result, errors.New("at least one L2 RPC is required"))
	}
	result = errors.Join(result, c.MetricsConfig.Check())
	result = errors.Join(result, c.RPC.Check())
	return result
}

func NewConfig(ctx *cli.Context, version string) (*Config, error) {
	l2rpcs, err := flags.ParseL2RPCs(ctx.String(flags.L2RPCsFlag.Name))
	if err != nil {
		return nil, err
	}

	return &Config{
		L2RPCs:        l2rpcs,
		DataDir:       ctx.String(flags.DataDirFlag.Name),
		BackfillHours: ctx.Uint64(flags.BackfillHoursFlag.Name),
		Version:       version,
		LogConfig:     oplog.ReadCLIConfig(ctx),
		MetricsConfig: opmetrics.ReadCLIConfig(ctx),
		PprofConfig:   oppprof.ReadCLIConfig(ctx),
		RPC:           oprpc.ReadCLIConfig(ctx),
	}, nil
}

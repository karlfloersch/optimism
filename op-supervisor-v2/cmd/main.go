package main

import (
	"context"
	"fmt"
	"os"

	"github.com/urfave/cli/v2"

	"github.com/ethereum-optimism/optimism/op-service/cliapp"
	oplog "github.com/ethereum-optimism/optimism/op-service/log"
	service "github.com/ethereum-optimism/optimism/op-supervisor-v2"
	sv2flags "github.com/ethereum-optimism/optimism/op-supervisor-v2/flags"
)

func main() {
	oplog.SetupDefaults()

	app := cli.NewApp()
	app.Version = "dev"
	app.Name = "op-supervisor-v2"
	app.Usage = "Supervisor v2: virtual op-node supervisor"
	app.Flags = cliapp.ProtectFlags(sv2flags.Flags)
	app.Action = cliapp.LifecycleCmd(SupervisorMain)

	if err := app.Run(os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func SupervisorMain(ctx *cli.Context, closeApp context.CancelCauseFunc) (cliapp.Lifecycle, error) {
	logCfg := oplog.ReadCLIConfig(ctx)
	logger := oplog.NewLogger(oplog.AppOut(ctx), logCfg)
	oplog.SetGlobalLogHandler(logger.Handler())

	cfg, err := service.NewConfig(ctx, logger)
	if err != nil {
		return nil, fmt.Errorf("unable to create SV2 config: %w", err)
	}
	cfg.Cancel = closeApp

	lc, err := service.New(ctx.Context, cfg, logger, "dev", nil)
	if err != nil {
		return nil, fmt.Errorf("unable to create SV2 service: %w", err)
	}
	return lc, nil
}

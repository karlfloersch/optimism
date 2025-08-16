package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/urfave/cli/v2"

	"github.com/ethereum-optimism/optimism/op-node/rollup"
	oplog "github.com/ethereum-optimism/optimism/op-service/log"
	supervisor "github.com/ethereum-optimism/optimism/op-supervisor-v2/supervisor"
	"github.com/ethereum/go-ethereum/log"
)

func main() {
	app := &cli.App{
		Name:  "op-supervisor-v2",
		Usage: "Supervisor v2 prototype: runs embedded op-node and exposes health",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "http.addr", Value: "127.0.0.1", Usage: "HTTP listen address"},
			&cli.IntFlag{Name: "http.port", Value: 9750, Usage: "HTTP listen port"},
			&cli.BoolFlag{Name: "proxy.opnode", Value: true, Usage: "Expose embedded op-node RPC under /opnode/"},
			// Embedded op-node mode (always on)
			&cli.StringFlag{Name: "l1.rpc", Usage: "L1 execution RPC endpoint"},
			&cli.StringFlag{Name: "beacon.addr", Usage: "L1 beacon endpoint for blobs (e.g. http://localhost:5052)"},
			&cli.StringFlag{Name: "l2.authrpc", Usage: "L2 execution Engine API (auth RPC) endpoint"},
			&cli.StringFlag{Name: "l2.userrpc", Usage: "L2 execution user RPC endpoint (for reads)"},
			&cli.StringFlag{Name: "jwt.secret", Usage: "Path to 32-byte hex JWT secret file for L2 Engine API"},
			&cli.StringFlag{Name: "rollup.config", Usage: "Path to rollup config JSON"},
			&cli.DurationFlag{Name: "poll.interval", Value: 1 * time.Second, Usage: "Polling interval for rollup status"},
			&cli.UintFlag{Name: "confirm.depth", Value: 40, Usage: "L1 confirmation depth for cross-safety gating"},
		},
		Action: func(ctx *cli.Context) error {
			// basic logger setup using op-service/log defaults
			logCfg := oplog.DefaultCLIConfig()
			lgr := oplog.NewLogger(os.Stdout, logCfg)
			oplog.SetGlobalLogHandler(oplog.NewLogHandler(os.Stdout, logCfg))

			httpAddr := ctx.String("http.addr")
			httpPort := ctx.Int("http.port")

			sup := supervisor.NewSupervisor(lgr)
			sup.EnableOpNodeProxy(ctx.Bool("proxy.opnode"))

			// Start HTTP server
			httpSrv := &http.Server{Addr: fmt.Sprintf("%s:%d", httpAddr, httpPort), Handler: sup.HTTPHandler()}
			go func() {
				log.Info("starting http server", "addr", httpSrv.Addr)
				if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
					log.Error("http server error", "err", err)
				}
			}()

			pollInt := ctx.Duration("poll.interval")
			confirmDepth := ctx.Uint("confirm.depth")

			// Embedded op-node only
			l1RPC := ctx.String("l1.rpc")
			beacon := ctx.String("beacon.addr")
			l2Auth := ctx.String("l2.authrpc")
			l2User := ctx.String("l2.userrpc")
			jwtPath := ctx.String("jwt.secret")
			rollupPath := ctx.String("rollup.config")
			if l1RPC == "" || beacon == "" || l2Auth == "" || l2User == "" || jwtPath == "" || rollupPath == "" {
				return fmt.Errorf("requires --l1.rpc, --beacon.addr, --l2.authrpc, --l2.userrpc, --jwt.secret, --rollup.config")
			}
			// Read JWT
			data, err := os.ReadFile(jwtPath)
			if err != nil {
				return fmt.Errorf("read jwt.secret: %w", err)
			}
			s := strings.TrimSpace(string(data))
			s = strings.TrimPrefix(s, "0x")
			b, err := hex.DecodeString(s)
			if err != nil {
				return fmt.Errorf("decode jwt.secret: %w", err)
			}
			if len(b) != 32 {
				return fmt.Errorf("jwt.secret must be 32 bytes, got %d", len(b))
			}
			var jwt [32]byte
			copy(jwt[:], b)
			// Read rollup config JSON
			cfgBytes, err := os.ReadFile(rollupPath)
			if err != nil {
				return fmt.Errorf("read rollup.config: %w", err)
			}
			var rcfg rollup.Config
			if err := json.Unmarshal(cfgBytes, &rcfg); err != nil {
				return fmt.Errorf("parse rollup.config: %w", err)
			}
			if err := sup.StartManaged(l1RPC, beacon, l2Auth, l2User, jwt, &rcfg, pollInt, uint64(confirmDepth)); err != nil {
				return fmt.Errorf("start embedded: %w", err)
			}

			// Wait for interrupt
			sigC := make(chan os.Signal, 1)
			signal.Notify(sigC, syscall.SIGINT, syscall.SIGTERM)
			<-sigC

			// Graceful shutdown
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = httpSrv.Shutdown(shutdownCtx)
			sup.Stop()
			return nil
		},
	}

	if err := app.Run(os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

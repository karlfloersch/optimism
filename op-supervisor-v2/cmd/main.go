package main

import (
    "context"
    "fmt"
    "net/http"
    "os"
    "os/signal"
    "strings"
    "syscall"
    "time"

    "github.com/urfave/cli/v2"

    oplog "github.com/ethereum-optimism/optimism/op-service/log"
    "github.com/ethereum/go-ethereum/log"
    supervisor "github.com/ethereum-optimism/optimism/op-supervisor-v2/supervisor"
)

func main() {
    app := &cli.App{
        Name:  "op-supervisor-v2",
        Usage: "Supervisor v2 prototype: manages op-node subprocess and exposes health",
        Flags: []cli.Flag{
            &cli.StringFlag{Name: "http.addr", Value: "127.0.0.1", Usage: "HTTP listen address"},
            &cli.IntFlag{Name: "http.port", Value: 9750, Usage: "HTTP listen port"},
            &cli.StringFlag{Name: "op-node.path", Usage: "Path to op-node binary (optional)"},
            &cli.StringFlag{Name: "op-node.args", Usage: "Comma-separated arguments to pass to op-node"},
            &cli.BoolFlag{Name: "no-op-node", Usage: "Do not start op-node (useful for tests)"},
        },
        Action: func(ctx *cli.Context) error {
            // basic logger setup using op-service/log defaults
            logCfg := oplog.DefaultCLIConfig()
            lgr := oplog.NewLogger(os.Stdout, logCfg)
            oplog.SetGlobalLogHandler(oplog.NewLogHandler(os.Stdout, logCfg))

            httpAddr := ctx.String("http.addr")
            httpPort := ctx.Int("http.port")
            noOpNode := ctx.Bool("no-op-node")

            sup := supervisor.NewSupervisor(lgr)

            // Start HTTP server
            httpSrv := &http.Server{Addr: fmt.Sprintf("%s:%d", httpAddr, httpPort), Handler: sup.HTTPHandler()}
            go func() {
                log.Info("starting http server", "addr", httpSrv.Addr)
                if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
                    log.Error("http server error", "err", err)
                }
            }()

            // Optionally start op-node subprocess
            if !noOpNode {
                bin := ctx.String("op-node.path")
                if bin == "" {
                    return fmt.Errorf("--op-node.path is required unless --no-op-node is set")
                }
                var args []string
                if s := ctx.String("op-node.args"); s != "" {
                    // split on commas, ignore empties
                    for _, p := range strings.Split(s, ",") {
                        if q := strings.TrimSpace(p); q != "" {
                            args = append(args, q)
                        }
                    }
                }
                if err := sup.StartOpNode(bin, args...); err != nil {
                    return fmt.Errorf("failed to start op-node: %w", err)
                }
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



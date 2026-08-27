// Command silhouette-bindings computes the canonical proof-batch commitments for a parsed rollup
// config and dependency set. Deployment tooling must use this command or silhouette.ComputeBindings
// rather than independently implementing either hash.
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/urfave/cli/v2"

	"github.com/ethereum-optimism/optimism/op-core/interop/depset"
	"github.com/ethereum-optimism/optimism/op-node/rollup"
	"github.com/ethereum-optimism/optimism/op-supernode/silhouette"
)

var (
	rollupConfigFlag = &cli.PathFlag{
		Name:      "rollup-config",
		Usage:     "Silhouette rollup config JSON",
		Required:  true,
		TakesFile: true,
	}
	dependencySetFlag = &cli.PathFlag{
		Name:      "dependency-set",
		Usage:     "Interop dependency-set JSON",
		Required:  true,
		TakesFile: true,
	}
	outFlag = &cli.PathFlag{
		Name:      "out",
		Usage:     "Write bindings JSON here instead of stdout",
		TakesFile: true,
	}
)

func main() {
	app := cli.NewApp()
	app.Name = "silhouette-bindings"
	app.Usage = "Compute canonical Silhouette configuration commitments"
	app.Flags = []cli.Flag{rollupConfigFlag, dependencySetFlag, outFlag}
	app.Action = run
	if err := app.Run(os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx *cli.Context) error {
	var rollupCfg rollup.Config
	if err := readJSON(ctx.Path(rollupConfigFlag.Name), &rollupCfg); err != nil {
		return fmt.Errorf("read rollup config: %w", err)
	}
	var depSet depset.StaticConfigDependencySet
	if err := readJSON(ctx.Path(dependencySetFlag.Name), &depSet); err != nil {
		return fmt.Errorf("read dependency set: %w", err)
	}
	bindings, err := silhouette.ComputeBindings(&rollupCfg, &depSet)
	if err != nil {
		return err
	}
	raw, err := json.MarshalIndent(bindings, "", "  ")
	if err != nil {
		return fmt.Errorf("encode bindings: %w", err)
	}
	raw = append(raw, '\n')
	if path := ctx.Path(outFlag.Name); path != "" {
		if err := os.WriteFile(path, raw, 0o644); err != nil {
			return fmt.Errorf("write bindings: %w", err)
		}
		return nil
	}
	_, err = os.Stdout.Write(raw)
	return err
}

func readJSON(path string, dest any) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dest); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return fmt.Errorf("unexpected trailing JSON value")
		}
		return fmt.Errorf("decode trailing data: %w", err)
	}
	return nil
}

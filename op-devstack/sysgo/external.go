package sysgo

import (
	"encoding/json"
	"math/big"
	"os"

	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/params"

	"github.com/ethereum-optimism/optimism/op-devstack/stack"
	"github.com/ethereum-optimism/optimism/op-node/rollup"
	"github.com/ethereum-optimism/optimism/op-service/eth"
)

// WithExternalL1NodesRPC registers an external L1 EL/CL pair by RPC endpoints without starting local nodes.
// It also registers an L1 network with a minimal chain config containing just the chain ID.
func WithExternalL1NodesRPC(l1ChainID eth.ChainID, l1ELRPC string, l1BeaconHTTP string) stack.Option[*Orchestrator] {
	return stack.AfterDeploy(func(orch *Orchestrator) {
		// Register minimal L1 network wrapper so L2 networks can link to it
		l1ID := stack.L1NetworkID(l1ChainID)
		cid, _ := l1ChainID.Uint64()
		gen := &core.Genesis{Config: &params.ChainConfig{ChainID: new(big.Int).SetUint64(cid)}}
		orch.l1Nets.Set(l1ChainID, &L1Network{id: l1ID, genesis: gen, blockTime: 12})

		// Register frontends pointing to external RPCs
		l1ELID := stack.NewL1ELNodeID("l1", l1ChainID)
		orch.l1ELs.Set(l1ELID, &L1ELNode{id: l1ELID, userRPC: l1ELRPC})

		l1CLID := stack.NewL1CLNodeID("l1", l1ChainID)
		orch.l1CLs.Set(l1CLID, &L1CLNode{id: l1CLID, beaconHTTPAddr: l1BeaconHTTP})
	})
}

// WithExternalL2FromFiles registers an L2 network using rollup config and genesis loaded from files.
func WithExternalL2FromFiles(l2ChainID eth.ChainID, l1ChainID eth.ChainID, rollupPath string, genesisPath string) stack.Option[*Orchestrator] {
	return stack.AfterDeploy(func(orch *Orchestrator) {
		p := orch.P()
		p.Require().True(rollupPath != "", "rollup config path required")
		p.Require().True(genesisPath != "", "genesis path required")

		var rcfg rollup.Config
		{
			f, err := os.Open(rollupPath)
			p.Require().NoError(err)
			defer f.Close()
			p.Require().NoError(json.NewDecoder(f).Decode(&rcfg))
		}

		var gen core.Genesis
		{
			f, err := os.Open(genesisPath)
			p.Require().NoError(err)
			defer f.Close()
			p.Require().NoError(json.NewDecoder(f).Decode(&gen))
		}

		l2Net := &L2Network{
			id:        stack.L2NetworkID(l2ChainID),
			l1ChainID: l1ChainID,
			genesis:   &gen,
			rollupCfg: &rcfg,
			keys:      orch.keys,
		}
		orch.l2Nets.Set(l2ChainID, l2Net)
	})
}

// WithExternalPresetFromEnv composes external L1 + L2 wiring from environment variables:
// - OP_L1_CHAIN_ID (default 11155111)
// - OP_L1_RPC, OP_L1_BEACON_RPC
// - OP_L2_CHAIN_ID (default 901)
// - OP_L2_ROLLUP_PATH, OP_L2_GENESIS_PATH
func WithExternalPresetFromEnv() stack.Option[*Orchestrator] {
	return stack.Combine[*Orchestrator](
		stack.AfterDeploy(func(orch *Orchestrator) {
			l1CID := eth.ChainIDFromUInt64(11155111)
			if v := os.Getenv("OP_L1_CHAIN_ID"); v != "" {
				if n, ok := new(big.Int).SetString(v, 10); ok {
					l1CID = eth.ChainIDFromUInt64(n.Uint64())
				}
			}
			l2CID := DefaultL2AID
			if v := os.Getenv("OP_L2_CHAIN_ID"); v != "" {
				if n, ok := new(big.Int).SetString(v, 10); ok {
					l2CID = eth.ChainIDFromUInt64(n.Uint64())
				}
			}
			l1RPC := os.Getenv("OP_L1_RPC")
			l1Beacon := os.Getenv("OP_L1_BEACON_RPC")
			orch.P().Require().NotEmpty(l1RPC, "OP_L1_RPC required")
			orch.P().Require().NotEmpty(l1Beacon, "OP_L1_BEACON_RPC required")

			rollupPath := os.Getenv("OP_L2_ROLLUP_PATH")
			genesisPath := os.Getenv("OP_L2_GENESIS_PATH")
			orch.P().Require().NotEmpty(rollupPath, "OP_L2_ROLLUP_PATH required")
			orch.P().Require().NotEmpty(genesisPath, "OP_L2_GENESIS_PATH required")

			// Evaluate the composed options immediately to preserve order
			WithExternalL1NodesRPC(l1CID, l1RPC, l1Beacon).AfterDeploy(orch)
			WithExternalL2FromFiles(l2CID, l1CID, rollupPath, genesisPath).AfterDeploy(orch)
		}),
	)
}

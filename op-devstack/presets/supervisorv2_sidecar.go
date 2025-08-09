package presets

import (
    "time"

    supv2 "github.com/ethereum-optimism/optimism/op-supervisor-v2/supervisor"
    "github.com/ethereum-optimism/optimism/op-devstack/stack"
    "github.com/ethereum-optimism/optimism/op-devstack/stack/match"
)

// WithSupervisorV2Sidecar adds a PostHydrate hook that starts a single op-supervisor-v2
// instance as a sidecar attached to the first L2 network in the system. It wires the
// supervisor to the L2 CL Rollup API and the L2 EL RPC and starts the polling loop.
//
// Note: This variant does not manage the op-node process; devstack continues to own
// op-node. The supervisor only polls heads and fetches receipts for ingestion.
func WithSupervisorV2Sidecar() stack.CommonOption {
    return stack.PostHydrate[stack.Orchestrator](func(sys stack.System) {
        t := sys.T()
        l2Nets := sys.L2Networks()
        if len(l2Nets) == 0 {
            t.Log("WithSupervisorV2Sidecar: no L2 networks; skipping sidecar startup")
            return
        }
        l2 := l2Nets[0]

        // Use the first CL (sequencer) and its corresponding EL engine
        cl := l2.L2CLNode(match.FirstL2CL)
        el := l2.L2ELNode(match.EngineFor(cl))

        roll := cl.RollupAPI()
        elRPC := el.L2EthClient().RPC()
        rcfg := l2.RollupConfig()

        s := supv2.NewSupervisor(t.Logger())
        if err := s.StartPollingWithRollupClient(roll, elRPC, rcfg, time.Second, 40); err != nil {
            t.Errorf("failed to start supervisor-v2 polling: %v", err)
            return
        }
        // Ensure the sidecar is stopped when the test/system ends
        t.Cleanup(func() { s.Stop() })
    })
}
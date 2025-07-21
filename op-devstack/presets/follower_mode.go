package presets

import (
	"github.com/ethereum/go-ethereum/log"

	"github.com/ethereum-optimism/optimism/op-chain-ops/devkeys"
	"github.com/ethereum-optimism/optimism/op-devstack/devtest"
	"github.com/ethereum-optimism/optimism/op-devstack/dsl"
	"github.com/ethereum-optimism/optimism/op-devstack/shim"
	"github.com/ethereum-optimism/optimism/op-devstack/stack"
	"github.com/ethereum-optimism/optimism/op-devstack/stack/match"
	"github.com/ethereum-optimism/optimism/op-devstack/sysgo"
	"github.com/ethereum-optimism/optimism/op-node/config"
)

type FollowerMode struct {
	Log          log.Logger
	T            devtest.T
	ControlPlane stack.ControlPlane

	L1Network *dsl.L1Network
	L1EL      *dsl.L1ELNode

	L2Chain   *dsl.L2Network
	L2Batcher *dsl.L2Batcher

	// Prover node (sequencer in prover mode - gossips safe heads)
	ProverEL *dsl.L2ELNode
	ProverCL *dsl.L2CLNode

	// Follower node (verifier in follower mode - receives safe heads)
	FollowerEL *dsl.L2ELNode
	FollowerCL *dsl.L2CLNode

	TestSequencer *dsl.TestSequencer

	Wallet *dsl.HDWallet

	FaucetL1 *dsl.Faucet
	FaucetL2 *dsl.Faucet
	FunderL1 *dsl.Funder
	FunderL2 *dsl.Funder
}

func (f *FollowerMode) L2Networks() []*dsl.L2Network {
	return []*dsl.L2Network{
		f.L2Chain,
	}
}

func (f *FollowerMode) StandardBridge() *dsl.StandardBridge {
	return dsl.NewStandardBridge(f.T, f.L2Chain, nil, f.L1EL)
}

// WithFollowerMode specifies a system that meets the FollowerMode criteria.
// Sets up a single L2 chain with two nodes: one prover, one follower
func WithFollowerMode() stack.CommonOption {
	return stack.Combine(
		// Use the multi-supervisor interop system as base (gives us 2 L2CLs with P2P)
		stack.MakeCommon(sysgo.MultiSupervisorInteropSystem(&sysgo.MultiSupervisorInteropSystemIDs{})),
		// Configure the first L2CL (sequencer) as prover mode
		stack.MakeCommon(sysgo.WithL2CLOption(func(p devtest.P, id stack.L2CLNodeID, cfg *config.Config) {
			// Configure the main sequencer as prover
			if id.String() == "sequencer-901" { // DefaultL2AID chain sequencer
				p.Logger().Info("Configuring sequencer node in prover mode", "id", id)
				cfg.Driver.Mode = "prover"
			}
		})),
		// Configure the second L2CL (verifier) as follower mode
		stack.MakeCommon(sysgo.WithL2CLOption(func(p devtest.P, id stack.L2CLNodeID, cfg *config.Config) {
			// Configure the verifier as follower
			if id.String() == "verifier-901" { // DefaultL2AID chain verifier
				p.Logger().Info("Configuring verifier node in follower mode", "id", id)
				cfg.Driver.Mode = "follower"
			}
		})),
	)
}

func NewFollowerMode(t devtest.T) *FollowerMode {
	system := shim.NewSystem(t)
	orch := Orchestrator()
	orch.Hydrate(system)

	t.Gate().Equal(len(system.TestSequencers()), 1, "expected exactly one test sequencer")

	l1Net := system.L1Network(match.FirstL1Network)
	l2 := system.L2Network(match.Assume(t, match.L2ChainA))

	// Get the sequencer (prover) and first verifier (follower) nodes
	proverCL := l2.L2CLNode(match.Assume(t, match.WithSequencerActive(t.Ctx())))
	followerCL := l2.L2CLNode(match.Assume(t, match.SecondL2CL)) // Second L2CL node as follower

	proverEL := l2.L2ELNode(match.Assume(t, match.EngineFor(proverCL)))
	followerEL := l2.L2ELNode(match.Assume(t, match.SecondL2EL)) // Second L2EL node for follower

	out := &FollowerMode{
		Log:           t.Logger(),
		T:             t,
		ControlPlane:  orch.ControlPlane(),
		L1Network:     dsl.NewL1Network(l1Net),
		L1EL:          dsl.NewL1ELNode(l1Net.L1ELNode(match.Assume(t, match.FirstL1EL))),
		L2Chain:       dsl.NewL2Network(l2, orch.ControlPlane()),
		L2Batcher:     dsl.NewL2Batcher(l2.L2Batcher(match.Assume(t, match.FirstL2Batcher))),
		ProverEL:      dsl.NewL2ELNode(proverEL, orch.ControlPlane()),
		ProverCL:      dsl.NewL2CLNode(proverCL, orch.ControlPlane()),
		FollowerEL:    dsl.NewL2ELNode(followerEL, orch.ControlPlane()),
		FollowerCL:    dsl.NewL2CLNode(followerCL, orch.ControlPlane()),
		TestSequencer: dsl.NewTestSequencer(system.TestSequencer(match.Assume(t, match.FirstTestSequencer))),
		Wallet:        dsl.NewHDWallet(t, devkeys.TestMnemonic, 30),
	}

	out.FaucetL1 = dsl.NewFaucet(out.L1Network.Escape().Faucet(match.Assume(t, match.FirstFaucet)))
	out.FaucetL2 = dsl.NewFaucet(l2.Faucet(match.Assume(t, match.FirstFaucet)))
	out.FunderL1 = dsl.NewFunder(out.Wallet, out.FaucetL1, out.L1EL)
	out.FunderL2 = dsl.NewFunder(out.Wallet, out.FaucetL2, out.ProverEL)

	return out
}

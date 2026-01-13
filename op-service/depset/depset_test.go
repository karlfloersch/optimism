package depset

import (
	"testing"

	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/stretchr/testify/require"
)

// Helper to create ChainID from uint64
func chainID(id uint64) eth.ChainID {
	return eth.ChainIDFromUInt64(id)
}

// --- Graph Tests ---

func TestDependencyGraph_Basic(t *testing.T) {
	g := NewDependencyGraph(1000)

	// Add chains
	g.AddChain(chainID(1))
	g.AddChain(chainID(2))
	g.AddChain(chainID(3))

	require.True(t, g.HasChain(chainID(1)))
	require.True(t, g.HasChain(chainID(2)))
	require.True(t, g.HasChain(chainID(3)))
	require.False(t, g.HasChain(chainID(4)))

	require.Equal(t, 3, g.NumChains())
	require.Equal(t, uint64(1000), g.Timestamp())
}

func TestDependencyGraph_Dependencies(t *testing.T) {
	g := NewDependencyGraph(1000)

	// Chain 1 depends on 2 and 3
	g.SetDependencies(chainID(1), []eth.ChainID{chainID(2), chainID(3)})

	deps := g.Dependencies(chainID(1))
	require.Len(t, deps, 2)
	require.True(t, g.HasDependency(chainID(1), chainID(2)))
	require.True(t, g.HasDependency(chainID(1), chainID(3)))
	require.False(t, g.HasDependency(chainID(1), chainID(4)))

	// Chain 2 has no dependencies
	require.Empty(t, g.Dependencies(chainID(2)))
}

func TestBuildGraphAt(t *testing.T) {
	cfg := &TemporalDependencyConfig{
		Chains: []ChainDependencyConfig{
			{
				ChainID: chainID(1),
				DependencyUpdates: []DependencyUpdate{
					{Timestamp: 100, Dependencies: []eth.ChainID{chainID(2)}},
					{Timestamp: 200, Dependencies: []eth.ChainID{chainID(2), chainID(3)}},
				},
			},
			{
				ChainID: chainID(2),
				DependencyUpdates: []DependencyUpdate{
					{Timestamp: 100, Dependencies: []eth.ChainID{}},
				},
			},
			{
				ChainID: chainID(3),
				DependencyUpdates: []DependencyUpdate{
					{Timestamp: 150, Dependencies: []eth.ChainID{}},
				},
			},
		},
	}

	// At timestamp 50 (before any updates)
	g50 := BuildGraphAt(cfg, 50)
	require.Empty(t, g50.Dependencies(chainID(1)))

	// At timestamp 100
	g100 := BuildGraphAt(cfg, 100)
	deps := g100.Dependencies(chainID(1))
	require.Len(t, deps, 1)
	require.Equal(t, chainID(2), deps[0])

	// At timestamp 200
	g200 := BuildGraphAt(cfg, 200)
	deps = g200.Dependencies(chainID(1))
	require.Len(t, deps, 2)
}

// --- Transitive Closure Tests ---

func TestTransitiveClosure_Simple(t *testing.T) {
	// A -> B -> C
	g := NewDependencyGraph(1000)
	g.SetDependencies(chainID(1), []eth.ChainID{chainID(2)})
	g.SetDependencies(chainID(2), []eth.ChainID{chainID(3)})
	g.AddChain(chainID(3))

	// Transitive closure of A should be [B, C]
	closure := TransitiveClosure(g, chainID(1))
	require.Len(t, closure, 2)
	require.Contains(t, closure, chainID(2))
	require.Contains(t, closure, chainID(3))

	// Transitive closure of B should be [C]
	closure = TransitiveClosure(g, chainID(2))
	require.Len(t, closure, 1)
	require.Equal(t, chainID(3), closure[0])

	// Transitive closure of C should be empty
	closure = TransitiveClosure(g, chainID(3))
	require.Empty(t, closure)
}

func TestTransitiveClosure_Diamond(t *testing.T) {
	//     A
	//    / \
	//   B   C
	//    \ /
	//     D
	g := NewDependencyGraph(1000)
	g.SetDependencies(chainID(1), []eth.ChainID{chainID(2), chainID(3)})
	g.SetDependencies(chainID(2), []eth.ChainID{chainID(4)})
	g.SetDependencies(chainID(3), []eth.ChainID{chainID(4)})
	g.AddChain(chainID(4))

	closure := TransitiveClosure(g, chainID(1))
	require.Len(t, closure, 3) // B, C, D
	require.Contains(t, closure, chainID(2))
	require.Contains(t, closure, chainID(3))
	require.Contains(t, closure, chainID(4))
}

func TestTransitiveClosure_Cycle(t *testing.T) {
	// A -> B -> C -> A (cycle)
	g := NewDependencyGraph(1000)
	g.SetDependencies(chainID(1), []eth.ChainID{chainID(2)})
	g.SetDependencies(chainID(2), []eth.ChainID{chainID(3)})
	g.SetDependencies(chainID(3), []eth.ChainID{chainID(1)})

	// All chains should reach all others
	closure := TransitiveClosure(g, chainID(1))
	require.Len(t, closure, 2) // B, C (not including self)
	require.Contains(t, closure, chainID(2))
	require.Contains(t, closure, chainID(3))
}

func TestMissingTransitiveDependencies(t *testing.T) {
	// A declares [B] but B depends on C
	// So A is missing transitive dependency on C
	g := NewDependencyGraph(1000)
	g.SetDependencies(chainID(1), []eth.ChainID{chainID(2)})
	g.SetDependencies(chainID(2), []eth.ChainID{chainID(3)})
	g.AddChain(chainID(3))

	missing := MissingTransitiveDependencies(g, chainID(1))
	require.Len(t, missing, 1)
	require.Equal(t, chainID(3), missing[0])

	// B is not missing any (declares nothing, needs nothing transitively not declared)
	// Actually B declares [C], so it has C transitively but also declares C
	// Wait, B declares [3] and 3 has no deps, so B's transitive is just [3] which it declares
	missing = MissingTransitiveDependencies(g, chainID(2))
	require.Empty(t, missing)
}

// --- Validation Tests ---

func TestValidate_ValidConfig(t *testing.T) {
	// A depends on B and C, B depends on C
	// A explicitly declares both B and C - valid!
	cfg := &TemporalDependencyConfig{
		Chains: []ChainDependencyConfig{
			{
				ChainID: chainID(1),
				DependencyUpdates: []DependencyUpdate{
					{Timestamp: 100, Dependencies: []eth.ChainID{chainID(2), chainID(3)}},
				},
			},
			{
				ChainID: chainID(2),
				DependencyUpdates: []DependencyUpdate{
					{Timestamp: 100, Dependencies: []eth.ChainID{chainID(3)}},
				},
			},
			{
				ChainID: chainID(3),
				DependencyUpdates: []DependencyUpdate{
					{Timestamp: 100, Dependencies: []eth.ChainID{}},
				},
			},
		},
	}

	err := Validate(cfg)
	require.NoError(t, err)
}

func TestValidate_MissingTransitiveDep(t *testing.T) {
	// A depends on B, B depends on C
	// A does NOT declare C - invalid!
	cfg := &TemporalDependencyConfig{
		Chains: []ChainDependencyConfig{
			{
				ChainID: chainID(1),
				DependencyUpdates: []DependencyUpdate{
					{Timestamp: 100, Dependencies: []eth.ChainID{chainID(2)}}, // Missing chainID(3)!
				},
			},
			{
				ChainID: chainID(2),
				DependencyUpdates: []DependencyUpdate{
					{Timestamp: 100, Dependencies: []eth.ChainID{chainID(3)}},
				},
			},
			{
				ChainID: chainID(3),
				DependencyUpdates: []DependencyUpdate{
					{Timestamp: 100, Dependencies: []eth.ChainID{}},
				},
			},
		},
	}

	err := Validate(cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing transitive dependencies")
}

func TestValidate_SelfDependency(t *testing.T) {
	cfg := &TemporalDependencyConfig{
		Chains: []ChainDependencyConfig{
			{
				ChainID: chainID(1),
				DependencyUpdates: []DependencyUpdate{
					{Timestamp: 100, Dependencies: []eth.ChainID{chainID(1)}}, // Self-dependency!
				},
			},
		},
	}

	err := Validate(cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot depend on itself")
}

func TestValidate_DuplicateChain(t *testing.T) {
	cfg := &TemporalDependencyConfig{
		Chains: []ChainDependencyConfig{
			{
				ChainID: chainID(1),
				DependencyUpdates: []DependencyUpdate{
					{Timestamp: 100, Dependencies: []eth.ChainID{}},
				},
			},
			{
				ChainID: chainID(1), // Duplicate!
				DependencyUpdates: []DependencyUpdate{
					{Timestamp: 200, Dependencies: []eth.ChainID{}},
				},
			},
		},
	}

	err := Validate(cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "duplicate chain definition")
}

func TestValidate_NonIncreasingTimestamps(t *testing.T) {
	cfg := &TemporalDependencyConfig{
		Chains: []ChainDependencyConfig{
			{
				ChainID: chainID(1),
				DependencyUpdates: []DependencyUpdate{
					{Timestamp: 200, Dependencies: []eth.ChainID{}},
					{Timestamp: 100, Dependencies: []eth.ChainID{}}, // Out of order!
				},
			},
		},
	}

	err := Validate(cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "strictly increasing timestamps")
}

// --- SCC Tests ---

func TestComputeSCCs_NoCycle(t *testing.T) {
	// A -> B -> C (no cycle)
	g := NewDependencyGraph(1000)
	g.SetDependencies(chainID(1), []eth.ChainID{chainID(2)})
	g.SetDependencies(chainID(2), []eth.ChainID{chainID(3)})
	g.AddChain(chainID(3))

	sccs := ComputeSCCs(g)
	require.Len(t, sccs, 3) // Each chain is its own SCC

	// All SCCs should be trivial (size 1, no self-loop)
	for _, scc := range sccs {
		require.True(t, scc.IsTrivial(g))
	}
}

func TestComputeSCCs_SimpleCycle(t *testing.T) {
	// A -> B -> A (cycle)
	g := NewDependencyGraph(1000)
	g.SetDependencies(chainID(1), []eth.ChainID{chainID(2)})
	g.SetDependencies(chainID(2), []eth.ChainID{chainID(1)})

	sccs := ComputeSCCs(g)
	require.Len(t, sccs, 1) // Both chains in same SCC

	scc := sccs[0]
	require.Len(t, scc.Chains, 2)
	require.True(t, scc.Contains(chainID(1)))
	require.True(t, scc.Contains(chainID(2)))
	require.False(t, scc.IsTrivial(g))
}

func TestComputeSCCs_TwoCycles(t *testing.T) {
	// A <-> B and C <-> D (two separate cycles)
	g := NewDependencyGraph(1000)
	g.SetDependencies(chainID(1), []eth.ChainID{chainID(2)})
	g.SetDependencies(chainID(2), []eth.ChainID{chainID(1)})
	g.SetDependencies(chainID(3), []eth.ChainID{chainID(4)})
	g.SetDependencies(chainID(4), []eth.ChainID{chainID(3)})

	sccs := ComputeSCCs(g)
	require.Len(t, sccs, 2) // Two SCCs

	// Both SCCs should have 2 chains
	for _, scc := range sccs {
		require.Len(t, scc.Chains, 2)
		require.False(t, scc.IsTrivial(g))
	}
}

// --- Interop Set Tests ---

func TestDeriveInteropSets_MutualDependency(t *testing.T) {
	// A <-> B (mutual dependency = interop set)
	g := NewDependencyGraph(1000)
	g.SetDependencies(chainID(1), []eth.ChainID{chainID(2)})
	g.SetDependencies(chainID(2), []eth.ChainID{chainID(1)})

	sets := DeriveInteropSets(g)
	require.Len(t, sets, 1)

	is := sets[0]
	require.Len(t, is.Chains, 2)
	require.True(t, is.Contains(chainID(1)))
	require.True(t, is.Contains(chainID(2)))
}

func TestDeriveInteropSets_NoMutualDependency(t *testing.T) {
	// A -> B (one-way = no interop set)
	g := NewDependencyGraph(1000)
	g.SetDependencies(chainID(1), []eth.ChainID{chainID(2)})
	g.AddChain(chainID(2))

	sets := DeriveInteropSets(g)
	require.Empty(t, sets) // No interop sets
}

func TestFindInteropSet(t *testing.T) {
	// A <-> B, C alone
	g := NewDependencyGraph(1000)
	g.SetDependencies(chainID(1), []eth.ChainID{chainID(2)})
	g.SetDependencies(chainID(2), []eth.ChainID{chainID(1)})
	g.AddChain(chainID(3))

	// A is in an interop set
	is := FindInteropSet(g, chainID(1))
	require.NotNil(t, is)
	require.True(t, is.Contains(chainID(1)))
	require.True(t, is.Contains(chainID(2)))

	// C is not in any interop set
	is = FindInteropSet(g, chainID(3))
	require.Nil(t, is)
}

// --- Temporal Tests ---

func TestTemporalDependencySet_DependenciesAt(t *testing.T) {
	cfg := &TemporalDependencyConfig{
		Chains: []ChainDependencyConfig{
			{
				ChainID: chainID(1),
				DependencyUpdates: []DependencyUpdate{
					{Timestamp: 100, Dependencies: []eth.ChainID{chainID(2)}},
					{Timestamp: 200, Dependencies: []eth.ChainID{chainID(2), chainID(3)}},
				},
			},
			{
				ChainID: chainID(2),
				DependencyUpdates: []DependencyUpdate{
					{Timestamp: 100, Dependencies: []eth.ChainID{}},
				},
			},
			{
				ChainID: chainID(3),
				DependencyUpdates: []DependencyUpdate{
					{Timestamp: 100, Dependencies: []eth.ChainID{}},
				},
			},
		},
	}

	tds := NewTemporalDependencySet(cfg)

	// At timestamp 50 (before any updates)
	deps := tds.DependenciesAt(chainID(1), 50)
	require.Empty(t, deps)

	// At timestamp 150
	deps = tds.DependenciesAt(chainID(1), 150)
	require.Len(t, deps, 1)
	require.Equal(t, chainID(2), deps[0])

	// At timestamp 250
	deps = tds.DependenciesAt(chainID(1), 250)
	require.Len(t, deps, 2)
}

func TestTemporalDependencySet_InteropSetAt(t *testing.T) {
	// At t=100: A -> B (no interop set)
	// At t=200: A <-> B (interop set!)
	cfg := &TemporalDependencyConfig{
		Chains: []ChainDependencyConfig{
			{
				ChainID: chainID(1),
				DependencyUpdates: []DependencyUpdate{
					{Timestamp: 100, Dependencies: []eth.ChainID{chainID(2)}},
				},
			},
			{
				ChainID: chainID(2),
				DependencyUpdates: []DependencyUpdate{
					{Timestamp: 100, Dependencies: []eth.ChainID{}},
					{Timestamp: 200, Dependencies: []eth.ChainID{chainID(1)}},
				},
			},
		},
	}

	tds := NewTemporalDependencySet(cfg)

	// At t=150: A -> B (one-way, no interop set)
	is := tds.InteropSetAt(chainID(1), 150)
	require.Nil(t, is)

	// At t=250: A <-> B (mutual, interop set!)
	is = tds.InteropSetAt(chainID(1), 250)
	require.NotNil(t, is)
	require.Len(t, is, 2)
}

func TestNewValidatedTemporalDependencySet_Invalid(t *testing.T) {
	// Invalid: A -> B -> C but A only declares B
	cfg := &TemporalDependencyConfig{
		Chains: []ChainDependencyConfig{
			{
				ChainID: chainID(1),
				DependencyUpdates: []DependencyUpdate{
					{Timestamp: 100, Dependencies: []eth.ChainID{chainID(2)}},
				},
			},
			{
				ChainID: chainID(2),
				DependencyUpdates: []DependencyUpdate{
					{Timestamp: 100, Dependencies: []eth.ChainID{chainID(3)}},
				},
			},
			{
				ChainID: chainID(3),
				DependencyUpdates: []DependencyUpdate{
					{Timestamp: 100, Dependencies: []eth.ChainID{}},
				},
			},
		},
	}

	_, err := NewValidatedTemporalDependencySet(cfg)
	require.Error(t, err)
}

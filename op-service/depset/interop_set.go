package depset

import (
	"github.com/ethereum-optimism/optimism/op-service/eth"
)

// InteropSet represents a set of chains that can freely interoperate.
// Chains in the same interop set form a strongly connected component,
// meaning each chain can read from and write to every other chain in the set.
type InteropSet struct {
	// Chains in this interop set, sorted by chain ID for determinism.
	Chains []eth.ChainID
}

// Size returns the number of chains in the interop set.
func (is *InteropSet) Size() int {
	return len(is.Chains)
}

// Contains returns true if the interop set contains the given chain.
func (is *InteropSet) Contains(chainID eth.ChainID) bool {
	for _, c := range is.Chains {
		if c.Cmp(chainID) == 0 {
			return true
		}
	}
	return false
}

// DeriveInteropSets derives interop sets from the SCCs of the dependency graph.
// Only non-trivial SCCs (size > 1 or with self-dependency) are considered interop sets.
// Single isolated chains without self-dependency are not interop sets because
// there's no mutual interoperability.
func DeriveInteropSets(g *DependencyGraph) []InteropSet {
	sccs := ComputeSCCs(g)
	interopSets := make([]InteropSet, 0)

	for _, scc := range sccs {
		// Skip trivial SCCs (single chain with no self-loop)
		if scc.IsTrivial(g) {
			continue
		}

		is := InteropSet{
			Chains: make([]eth.ChainID, len(scc.Chains)),
		}
		copy(is.Chains, scc.Chains)
		interopSets = append(interopSets, is)
	}

	return interopSets
}

// FindInteropSet finds the interop set containing the given chain.
// Returns nil if the chain is not part of any interop set.
func FindInteropSet(g *DependencyGraph, chainID eth.ChainID) *InteropSet {
	interopSets := DeriveInteropSets(g)
	for i := range interopSets {
		if interopSets[i].Contains(chainID) {
			return &interopSets[i]
		}
	}
	return nil
}

// AllInteropSets returns all interop sets in the graph.
// This is an alias for DeriveInteropSets for API clarity.
func AllInteropSets(g *DependencyGraph) []InteropSet {
	return DeriveInteropSets(g)
}

// InteropSetChains returns the chain IDs in the interop set containing the given chain.
// Returns nil if the chain is not part of any interop set.
func InteropSetChains(g *DependencyGraph, chainID eth.ChainID) []eth.ChainID {
	is := FindInteropSet(g, chainID)
	if is == nil {
		return nil
	}
	result := make([]eth.ChainID, len(is.Chains))
	copy(result, is.Chains)
	return result
}

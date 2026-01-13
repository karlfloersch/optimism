package depset

import (
	"github.com/ethereum-optimism/optimism/op-service/eth"
)

// TransitiveClosure computes the transitive closure of all dependencies for a single chain.
// Returns all chains that are reachable from the given chain (directly or transitively).
// The result includes only the reachable chains, not the source chain itself.
//
// For example, if A → B → C, then TransitiveClosure(graph, A) returns [B, C].
// If there's a cycle A → B → C → A, the result is still [B, C] (excluding the source).
func TransitiveClosure(g *DependencyGraph, chainID eth.ChainID) []eth.ChainID {
	if !g.HasChain(chainID) {
		return nil
	}

	visited := make(map[eth.ChainID]struct{})
	var result []eth.ChainID

	// Mark the source as visited to exclude it from results
	visited[chainID] = struct{}{}

	// DFS to find all reachable chains
	var dfs func(current eth.ChainID)
	dfs = func(current eth.ChainID) {
		deps := g.Dependencies(current)
		for _, dep := range deps {
			if _, seen := visited[dep]; !seen {
				visited[dep] = struct{}{}
				result = append(result, dep)
				dfs(dep)
			}
		}
	}

	dfs(chainID)
	eth.SortChainID(result)
	return result
}

// ComputeAllTransitiveClosures computes the transitive closure for every chain in the graph.
// Returns a map from chain ID to the list of all chains it transitively depends on.
func ComputeAllTransitiveClosures(g *DependencyGraph) map[eth.ChainID][]eth.ChainID {
	result := make(map[eth.ChainID][]eth.ChainID)
	for _, chainID := range g.Chains() {
		result[chainID] = TransitiveClosure(g, chainID)
	}
	return result
}

// chainIDSet is a helper for set operations on chain IDs.
type chainIDSet map[eth.ChainID]struct{}

func newChainIDSet(ids []eth.ChainID) chainIDSet {
	s := make(chainIDSet)
	for _, id := range ids {
		s[id] = struct{}{}
	}
	return s
}

func (s chainIDSet) contains(id eth.ChainID) bool {
	_, ok := s[id]
	return ok
}

func (s chainIDSet) toSlice() []eth.ChainID {
	result := make([]eth.ChainID, 0, len(s))
	for id := range s {
		result = append(result, id)
	}
	eth.SortChainID(result)
	return result
}

// MissingTransitiveDependencies returns the transitive dependencies of a chain
// that are not explicitly declared in its direct dependencies.
//
// For example, if A declares dependencies [B] but B depends on C,
// and A doesn't declare C, then C is a missing transitive dependency.
func MissingTransitiveDependencies(g *DependencyGraph, chainID eth.ChainID) []eth.ChainID {
	declared := newChainIDSet(g.Dependencies(chainID))
	transitive := newChainIDSet(TransitiveClosure(g, chainID))

	var missing []eth.ChainID
	for dep := range transitive {
		if !declared.contains(dep) {
			missing = append(missing, dep)
		}
	}

	eth.SortChainID(missing)
	return missing
}

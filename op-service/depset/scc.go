package depset

import (
	"github.com/ethereum-optimism/optimism/op-service/eth"
)

// SCC represents a Strongly Connected Component in the dependency graph.
// An SCC is a maximal set of vertices where every vertex is reachable from every other vertex.
type SCC struct {
	// Chains in this SCC, sorted by chain ID for determinism.
	Chains []eth.ChainID
}

// Size returns the number of chains in the SCC.
func (s *SCC) Size() int {
	return len(s.Chains)
}

// IsTrivial returns true if the SCC contains only one chain with no self-dependency.
// Trivial SCCs are not considered interop sets because there's no mutual dependency.
func (s *SCC) IsTrivial(g *DependencyGraph) bool {
	if len(s.Chains) != 1 {
		return false
	}
	// A single-node SCC is non-trivial only if there's a self-loop
	return !g.HasDependency(s.Chains[0], s.Chains[0])
}

// Contains returns true if the SCC contains the given chain.
func (s *SCC) Contains(chainID eth.ChainID) bool {
	for _, c := range s.Chains {
		if c.Cmp(chainID) == 0 {
			return true
		}
	}
	return false
}

// tarjanState holds the state for a single node during Tarjan's algorithm.
type tarjanState struct {
	index   int
	lowlink int
	onStack bool
}

// ComputeSCCs computes all strongly connected components in the dependency graph
// using Tarjan's algorithm. Returns SCCs in reverse topological order.
//
// Time complexity: O(V + E) where V is the number of chains and E is the number of edges.
func ComputeSCCs(g *DependencyGraph) []SCC {
	index := 0
	stack := make([]eth.ChainID, 0)
	state := make(map[eth.ChainID]*tarjanState)
	sccs := make([]SCC, 0)

	var strongconnect func(v eth.ChainID)
	strongconnect = func(v eth.ChainID) {
		// Set the depth index for v
		state[v] = &tarjanState{
			index:   index,
			lowlink: index,
			onStack: true,
		}
		index++
		stack = append(stack, v)

		// Consider successors of v
		for _, w := range g.Dependencies(v) {
			if state[w] == nil {
				// Successor w has not yet been visited; recurse on it
				strongconnect(w)
				state[v].lowlink = min(state[v].lowlink, state[w].lowlink)
			} else if state[w].onStack {
				// Successor w is in the stack and hence in the current SCC
				state[v].lowlink = min(state[v].lowlink, state[w].index)
			}
		}

		// If v is a root node, pop the stack and generate an SCC
		if state[v].lowlink == state[v].index {
			scc := SCC{Chains: make([]eth.ChainID, 0)}
			for {
				w := stack[len(stack)-1]
				stack = stack[:len(stack)-1]
				state[w].onStack = false
				scc.Chains = append(scc.Chains, w)
				if w.Cmp(v) == 0 {
					break
				}
			}
			// Sort chains for determinism
			eth.SortChainID(scc.Chains)
			sccs = append(sccs, scc)
		}
	}

	// Call strongconnect for each unvisited node
	for _, v := range g.Chains() {
		if state[v] == nil {
			strongconnect(v)
		}
	}

	return sccs
}

// min returns the minimum of two integers.
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

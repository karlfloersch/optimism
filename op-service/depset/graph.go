package depset

import (
	"github.com/ethereum-optimism/optimism/op-service/eth"
)

// DependencyGraph represents the dependency relationships at a specific timestamp.
// This is an immutable snapshot of the dependency graph at time T.
//
// In the graph, an edge from A to B means "A depends on B" (A can read B's state).
type DependencyGraph struct {
	// adjacency maps each chain to its direct dependencies.
	adjacency map[eth.ChainID][]eth.ChainID

	// chains is the set of all chains in the graph.
	chains map[eth.ChainID]struct{}

	// timestamp is the point in time this graph represents.
	timestamp uint64
}

// NewDependencyGraph creates a new empty dependency graph for the given timestamp.
func NewDependencyGraph(timestamp uint64) *DependencyGraph {
	return &DependencyGraph{
		adjacency: make(map[eth.ChainID][]eth.ChainID),
		chains:    make(map[eth.ChainID]struct{}),
		timestamp: timestamp,
	}
}

// AddChain adds a chain to the graph without any dependencies.
func (g *DependencyGraph) AddChain(chainID eth.ChainID) {
	g.chains[chainID] = struct{}{}
	if _, exists := g.adjacency[chainID]; !exists {
		g.adjacency[chainID] = nil
	}
}

// SetDependencies sets the dependencies for a chain, replacing any existing dependencies.
func (g *DependencyGraph) SetDependencies(chainID eth.ChainID, deps []eth.ChainID) {
	g.chains[chainID] = struct{}{}
	g.adjacency[chainID] = make([]eth.ChainID, len(deps))
	copy(g.adjacency[chainID], deps)

	// Also ensure all dependency chains are in the graph
	for _, dep := range deps {
		g.chains[dep] = struct{}{}
		if _, exists := g.adjacency[dep]; !exists {
			g.adjacency[dep] = nil
		}
	}
}

// Dependencies returns the direct dependencies of a chain.
// Returns nil if the chain is not in the graph.
func (g *DependencyGraph) Dependencies(chainID eth.ChainID) []eth.ChainID {
	deps, exists := g.adjacency[chainID]
	if !exists {
		return nil
	}
	// Return a copy to prevent mutation
	result := make([]eth.ChainID, len(deps))
	copy(result, deps)
	return result
}

// Chains returns all chains in the graph, sorted by chain ID.
func (g *DependencyGraph) Chains() []eth.ChainID {
	result := make([]eth.ChainID, 0, len(g.chains))
	for chainID := range g.chains {
		result = append(result, chainID)
	}
	eth.SortChainID(result)
	return result
}

// HasChain returns true if the chain is in the graph.
func (g *DependencyGraph) HasChain(chainID eth.ChainID) bool {
	_, exists := g.chains[chainID]
	return exists
}

// HasDependency returns true if 'from' has 'to' as a direct dependency.
func (g *DependencyGraph) HasDependency(from, to eth.ChainID) bool {
	deps := g.adjacency[from]
	for _, dep := range deps {
		if dep.Cmp(to) == 0 {
			return true
		}
	}
	return false
}

// Timestamp returns the timestamp this graph represents.
func (g *DependencyGraph) Timestamp() uint64 {
	return g.timestamp
}

// NumChains returns the number of chains in the graph.
func (g *DependencyGraph) NumChains() int {
	return len(g.chains)
}

// NumEdges returns the total number of dependency edges in the graph.
func (g *DependencyGraph) NumEdges() int {
	count := 0
	for _, deps := range g.adjacency {
		count += len(deps)
	}
	return count
}

// BuildGraphAt builds a DependencyGraph from a TemporalDependencyConfig at the given timestamp.
// For each chain, it finds the most recent DependencyUpdate with timestamp <= ts.
func BuildGraphAt(cfg *TemporalDependencyConfig, ts uint64) *DependencyGraph {
	g := NewDependencyGraph(ts)

	for _, chainCfg := range cfg.Chains {
		g.AddChain(chainCfg.ChainID)

		// Find the most recent update at or before ts
		var effectiveUpdate *DependencyUpdate
		for i := range chainCfg.DependencyUpdates {
			update := &chainCfg.DependencyUpdates[i]
			if update.Timestamp <= ts {
				effectiveUpdate = update
			} else {
				break // Updates are sorted, so we can stop
			}
		}

		if effectiveUpdate != nil {
			g.SetDependencies(chainCfg.ChainID, effectiveUpdate.Dependencies)
		}
	}

	return g
}

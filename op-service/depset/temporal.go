package depset

import (
	"sync"

	"github.com/ethereum-optimism/optimism/op-service/eth"
)

// TemporalDependencySet provides temporal queries on a validated dependency configuration.
// All methods are safe for concurrent use.
type TemporalDependencySet interface {
	// DependenciesAt returns the dependencies for a chain at a given timestamp.
	// Returns nil if the chain has no dependencies at that time or doesn't exist.
	DependenciesAt(chainID eth.ChainID, timestamp uint64) []eth.ChainID

	// InteropSetAt returns the interop set (SCC) containing the chain at timestamp.
	// Interop sets are strongly connected components - chains with mutual dependencies.
	// Returns nil if the chain is not part of any interop set at the given time.
	InteropSetAt(chainID eth.ChainID, timestamp uint64) []eth.ChainID

	// GraphAt returns the full dependency graph snapshot at the given timestamp.
	GraphAt(timestamp uint64) *DependencyGraph
}

// temporalDependencySet is the concrete implementation of TemporalDependencySet.
type temporalDependencySet struct {
	config *TemporalDependencyConfig

	// chainSet contains all chains in the config for quick lookup.
	chainSet map[eth.ChainID]struct{}

	// sortedTimestamps is a sorted list of all activation timestamps.
	sortedTimestamps []uint64

	// graphCache caches computed graphs at specific timestamps.
	graphCache sync.Map // map[uint64]*DependencyGraph
}

// NewTemporalDependencySet creates a new TemporalDependencySet from a validated config.
// The config must have been validated with Validate() before calling this.
// If validation hasn't been done, the behavior is undefined.
func NewTemporalDependencySet(cfg *TemporalDependencyConfig) TemporalDependencySet {
	impl := &temporalDependencySet{
		config:   cfg,
		chainSet: make(map[eth.ChainID]struct{}),
	}

	// Build chain set
	for _, chain := range cfg.Chains {
		impl.chainSet[chain.ChainID] = struct{}{}
	}

	// Get sorted timestamps
	impl.sortedTimestamps = cfg.AllTimestamps()

	return impl
}

// NewValidatedTemporalDependencySet creates a new TemporalDependencySet, validating first.
// Returns an error if validation fails.
func NewValidatedTemporalDependencySet(cfg *TemporalDependencyConfig) (TemporalDependencySet, error) {
	if err := Validate(cfg); err != nil {
		return nil, err
	}
	return NewTemporalDependencySet(cfg), nil
}

// DependenciesAt returns the dependencies for a chain at a given timestamp.
func (t *temporalDependencySet) DependenciesAt(chainID eth.ChainID, timestamp uint64) []eth.ChainID {
	g := t.GraphAt(timestamp)
	return g.Dependencies(chainID)
}

// InteropSetAt returns the interop set containing the chain at the given timestamp.
func (t *temporalDependencySet) InteropSetAt(chainID eth.ChainID, timestamp uint64) []eth.ChainID {
	g := t.GraphAt(timestamp)
	return InteropSetChains(g, chainID)
}

// GraphAt returns the dependency graph at the given timestamp.
// Results are cached for efficiency.
func (t *temporalDependencySet) GraphAt(timestamp uint64) *DependencyGraph {
	// Check cache first
	if cached, ok := t.graphCache.Load(timestamp); ok {
		return cached.(*DependencyGraph)
	}

	// Build the graph
	g := BuildGraphAt(t.config, timestamp)

	// Cache and return
	t.graphCache.Store(timestamp, g)
	return g
}

// Chains returns all chains in the configuration.
func (t *temporalDependencySet) Chains() []eth.ChainID {
	return t.config.ChainIDs()
}

// HasChain returns true if the chain is in the configuration.
func (t *temporalDependencySet) HasChain(chainID eth.ChainID) bool {
	_, exists := t.chainSet[chainID]
	return exists
}

// ActivationTimestamps returns all activation timestamps in ascending order.
func (t *temporalDependencySet) ActivationTimestamps() []uint64 {
	result := make([]uint64, len(t.sortedTimestamps))
	copy(result, t.sortedTimestamps)
	return result
}

package depset

import (
	"github.com/ethereum-optimism/optimism/op-service/eth"
)

// DependencyUpdate represents a timestamped dependency change (DSO fork).
// At the given timestamp, the chain's dependency set becomes the specified list.
type DependencyUpdate struct {
	// Timestamp is the Unix timestamp when this dependency set becomes active.
	Timestamp uint64 `json:"timestamp" toml:"timestamp"`

	// Dependencies is the list of chain IDs that this chain depends on at this timestamp.
	// All transitive dependencies must be explicitly listed.
	Dependencies []eth.ChainID `json:"dependencies" toml:"dependencies"`
}

// ChainDependencyConfig holds all dependency updates for a single chain.
type ChainDependencyConfig struct {
	// ChainID is the identifier of this chain.
	ChainID eth.ChainID `json:"chain_id" toml:"chain_id"`

	// DependencyUpdates is a list of timestamped dependency changes.
	// Must be sorted by timestamp in ascending order.
	DependencyUpdates []DependencyUpdate `json:"dependency_updates" toml:"dependency_updates"`
}

// TemporalDependencyConfig is the full multi-chain dependency configuration.
// This is the top-level configuration structure that describes dependency
// relationships across all chains over time.
type TemporalDependencyConfig struct {
	// Chains is the list of chain configurations.
	Chains []ChainDependencyConfig `json:"chains" toml:"chains"`
}

// ChainDependencyConfigWithInteropTime pairs a chain's dependency config with its interop activation time.
// This is used for validation to ensure the first dependency_updates timestamp matches interop_time.
type ChainDependencyConfigWithInteropTime struct {
	ChainDependencyConfig

	// InteropTime is the timestamp when interop activates for this chain (from hardforks section).
	// The first DependencyUpdate timestamp must match this value.
	InteropTime uint64 `json:"interop_time" toml:"interop_time"`
}

// ChainIDs returns the list of all chain IDs in the configuration.
func (c *TemporalDependencyConfig) ChainIDs() []eth.ChainID {
	ids := make([]eth.ChainID, len(c.Chains))
	for i, chain := range c.Chains {
		ids[i] = chain.ChainID
	}
	return ids
}

// AllTimestamps returns all unique activation timestamps across all chains, sorted ascending.
func (c *TemporalDependencyConfig) AllTimestamps() []uint64 {
	seen := make(map[uint64]struct{})
	for _, chain := range c.Chains {
		for _, update := range chain.DependencyUpdates {
			seen[update.Timestamp] = struct{}{}
		}
	}

	timestamps := make([]uint64, 0, len(seen))
	for ts := range seen {
		timestamps = append(timestamps, ts)
	}

	// Sort ascending
	for i := 0; i < len(timestamps)-1; i++ {
		for j := i + 1; j < len(timestamps); j++ {
			if timestamps[i] > timestamps[j] {
				timestamps[i], timestamps[j] = timestamps[j], timestamps[i]
			}
		}
	}
	return timestamps
}

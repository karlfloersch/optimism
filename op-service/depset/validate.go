package depset

import (
	"errors"
	"fmt"
	"strings"

	"github.com/ethereum-optimism/optimism/op-service/eth"
)

// ValidationError represents a validation failure with context.
type ValidationError struct {
	// Timestamp at which the validation failed (0 if not timestamp-specific)
	Timestamp uint64

	// ChainID that has the validation issue (nil if not chain-specific)
	ChainID *eth.ChainID

	// Missing contains the chain IDs that are missing from the declared dependencies
	Missing []eth.ChainID

	// Message describes the validation failure
	Message string
}

func (e *ValidationError) Error() string {
	var sb strings.Builder
	sb.WriteString(e.Message)

	if e.Timestamp > 0 {
		sb.WriteString(fmt.Sprintf(" (at timestamp %d)", e.Timestamp))
	}

	if e.ChainID != nil {
		sb.WriteString(fmt.Sprintf(" for chain %s", e.ChainID.String()))
	}

	if len(e.Missing) > 0 {
		sb.WriteString(": missing [")
		for i, m := range e.Missing {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(m.String())
		}
		sb.WriteString("]")
	}

	return sb.String()
}

// Validate checks a TemporalDependencyConfig for validity.
// It ensures that at every activation timestamp, all transitive dependencies
// are explicitly declared for every chain.
//
// Returns nil if the configuration is valid, or an error describing the issues.
func Validate(cfg *TemporalDependencyConfig) error {
	if cfg == nil {
		return errors.New("nil configuration")
	}

	if len(cfg.Chains) == 0 {
		return nil // Empty config is valid
	}

	// Validate structure first
	if err := validateStructure(cfg); err != nil {
		return err
	}

	// Validate at each activation timestamp
	timestamps := cfg.AllTimestamps()
	var errs []error

	for _, ts := range timestamps {
		if err := ValidateAtTimestamp(cfg, ts); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	return nil
}

// validateStructure checks the structural validity of the configuration.
func validateStructure(cfg *TemporalDependencyConfig) error {
	var errs []error

	seenChains := make(map[eth.ChainID]struct{})
	for _, chainCfg := range cfg.Chains {
		// Check for duplicate chain definitions
		if _, seen := seenChains[chainCfg.ChainID]; seen {
			errs = append(errs, &ValidationError{
				ChainID: &chainCfg.ChainID,
				Message: "duplicate chain definition",
			})
		}
		seenChains[chainCfg.ChainID] = struct{}{}

		// Check timestamps are in ascending order
		var prevTs uint64
		for i, update := range chainCfg.DependencyUpdates {
			if i > 0 && update.Timestamp <= prevTs {
				errs = append(errs, &ValidationError{
					ChainID:   &chainCfg.ChainID,
					Timestamp: update.Timestamp,
					Message:   "dependency updates must have strictly increasing timestamps",
				})
			}
			prevTs = update.Timestamp

			// Check for self-dependency
			for _, dep := range update.Dependencies {
				if dep.Cmp(chainCfg.ChainID) == 0 {
					errs = append(errs, &ValidationError{
						ChainID:   &chainCfg.ChainID,
						Timestamp: update.Timestamp,
						Message:   "chain cannot depend on itself",
					})
				}
			}
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	return nil
}

// ValidateAtTimestamp validates the dependency graph at a specific timestamp.
// It checks that all transitive dependencies are explicitly declared.
func ValidateAtTimestamp(cfg *TemporalDependencyConfig, ts uint64) error {
	graph := BuildGraphAt(cfg, ts)
	var errs []error

	for _, chainID := range graph.Chains() {
		missing := MissingTransitiveDependencies(graph, chainID)
		if len(missing) > 0 {
			chainCopy := chainID
			errs = append(errs, &ValidationError{
				Timestamp: ts,
				ChainID:   &chainCopy,
				Missing:   missing,
				Message:   "missing transitive dependencies",
			})
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	return nil
}

// ValidateGraph validates a single DependencyGraph for explicit transitivity.
// This is useful when you already have a graph and don't need temporal validation.
func ValidateGraph(g *DependencyGraph) error {
	var errs []error

	for _, chainID := range g.Chains() {
		missing := MissingTransitiveDependencies(g, chainID)
		if len(missing) > 0 {
			chainCopy := chainID
			errs = append(errs, &ValidationError{
				Timestamp: g.Timestamp(),
				ChainID:   &chainCopy,
				Missing:   missing,
				Message:   "missing transitive dependencies",
			})
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	return nil
}

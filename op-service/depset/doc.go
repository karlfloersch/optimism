// Package depset provides utilities for managing and validating interop dependency sets.
//
// Each chain in the interop protocol specifies a dependency set: the set of other chains
// whose state it is allowed to read. Dependencies are logically transitive (if A depends
// on B and B depends on C, then A implicitly depends on C), but the configuration requires
// all dependencies to be explicitly declared.
//
// This package provides:
//   - Temporal dependency configuration with timestamped updates (DSO forks)
//   - Validation that all transitive dependencies are explicitly declared
//   - Interop set computation (strongly connected components of mutual dependencies)
//   - Dependency graph operations and queries
//
// The package is designed to be used by services like op-interop-filter, supernode,
// and other components that need to understand chain dependency relationships.
package depset

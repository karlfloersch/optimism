package silhouette

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/common"

	"github.com/ethereum-optimism/optimism/op-core/interop/depset"
	"github.com/ethereum-optimism/optimism/op-node/rollup"
)

// Bindings are the consensus-critical artifact commitments carried by every proof batch.
// Both the verifier and submitter must use values produced from the same parsed artifacts.
type Bindings struct {
	RollupConfigHash common.Hash `json:"rollupConfigHash"`
	DepSetHash       common.Hash `json:"depSetHash"`
}

// ComputeBindings computes the canonical silhouette commitments from parsed configuration.
// Parsing before marshaling normalizes insignificant source formatting and prevents deployment
// tooling from independently defining a JSON canonicalization or hash rule.
func ComputeBindings(
	rollupCfg *rollup.Config,
	depSet *depset.StaticConfigDependencySet,
) (Bindings, error) {
	if rollupCfg == nil {
		return Bindings{}, errors.New("rollup config is required")
	}
	if err := rollupCfg.Check(); err != nil {
		return Bindings{}, fmt.Errorf("invalid rollup config: %w", err)
	}
	if depSet == nil {
		return Bindings{}, errors.New("dependency set is required")
	}

	rollupJSON, err := json.Marshal(rollupCfg)
	if err != nil {
		return Bindings{}, fmt.Errorf("encode parsed rollup config: %w", err)
	}
	depSetJSON, err := json.Marshal(depSet)
	if err != nil {
		return Bindings{}, fmt.Errorf("encode parsed dependency set: %w", err)
	}
	rollupHash := sha256.Sum256(rollupJSON)
	depSetHash := sha256.Sum256(depSetJSON)
	return Bindings{
		RollupConfigHash: common.Hash(rollupHash),
		DepSetHash:       common.Hash(depSetHash),
	}, nil
}

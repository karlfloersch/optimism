package silhouette

import (
	"encoding/json"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/require"

	"github.com/ethereum-optimism/optimism/op-core/interop/depset"
	"github.com/ethereum-optimism/optimism/op-node/rollup"
	"github.com/ethereum-optimism/optimism/op-service/eth"
)

func TestComputeBindingsUsesParsedArtifacts(t *testing.T) {
	cfg, err := RollupConfigFor(silhouetteParams())
	require.NoError(t, err)
	depSet, err := depset.NewStaticConfigDependencySet(map[eth.ChainID]*depset.StaticConfigDependency{
		eth.ChainIDFromUInt64(901): {},
		eth.ChainIDFromUInt64(902): {},
	})
	require.NoError(t, err)

	first, err := ComputeBindings(cfg, depSet)
	require.NoError(t, err)
	require.Equal(t,
		common.HexToHash("0xc31aa5801e577a08a004cbac55a44c1e580517bbc4fd59cb5229587b0c9b79f1"),
		first.RollupConfigHash)
	require.Equal(t,
		common.HexToHash("0x343a504cd220194b36ed907bbc644fbd58d2b3e32bcbbe271f11e17f50f1ba74"),
		first.DepSetHash)

	// Reparse differently formatted JSON before hashing. Source whitespace and key order must not
	// become part of a consensus commitment.
	cfgJSON, err := json.MarshalIndent(cfg, "", "    ")
	require.NoError(t, err)
	var reparsedCfg rollup.Config
	require.NoError(t, json.Unmarshal(cfgJSON, &reparsedCfg))
	depSetJSON, err := json.MarshalIndent(depSet, "", "    ")
	require.NoError(t, err)
	var reparsedDepSet depset.StaticConfigDependencySet
	require.NoError(t, json.Unmarshal(depSetJSON, &reparsedDepSet))

	second, err := ComputeBindings(&reparsedCfg, &reparsedDepSet)
	require.NoError(t, err)
	require.Equal(t, first, second)

	reparsedCfg.BlockTime++
	changed, err := ComputeBindings(&reparsedCfg, &reparsedDepSet)
	require.NoError(t, err)
	require.NotEqual(t, first.RollupConfigHash, changed.RollupConfigHash)
	require.Equal(t, first.DepSetHash, changed.DepSetHash)
}

func TestComputeBindingsRejectsMissingArtifacts(t *testing.T) {
	_, err := ComputeBindings(nil, nil)
	require.ErrorContains(t, err, "rollup config is required")

	cfg, cfgErr := RollupConfigFor(silhouetteParams())
	require.NoError(t, cfgErr)
	_, err = ComputeBindings(cfg, nil)
	require.ErrorContains(t, err, "dependency set is required")
}

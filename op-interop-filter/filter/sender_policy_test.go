package filter

import (
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/require"
)

func mustParseTestSenderPolicy(t *testing.T, raw string) *SenderPolicy {
	t.Helper()

	policy, err := ParseSenderPolicy(raw)
	require.NoError(t, err)
	return policy
}

func TestParseSenderPolicy(t *testing.T) {
	t.Run("wildcard", func(t *testing.T) {
		policy, err := ParseSenderPolicy("*")
		require.NoError(t, err)
		require.True(t, policy.AllowsAny())
		require.True(t, policy.Allows(common.HexToAddress("0x1111111111111111111111111111111111111111")))
	})

	t.Run("comma separated addresses", func(t *testing.T) {
		addrA := common.HexToAddress("0x1111111111111111111111111111111111111111")
		addrB := common.HexToAddress("0x2222222222222222222222222222222222222222")
		policy, err := ParseSenderPolicy(addrA.Hex() + "," + addrB.Hex())
		require.NoError(t, err)
		require.False(t, policy.AllowsAny())
		require.True(t, policy.Allows(addrA))
		require.True(t, policy.Allows(addrB))
		require.False(t, policy.Allows(common.HexToAddress("0x3333333333333333333333333333333333333333")))
	})

	t.Run("rejects empty input", func(t *testing.T) {
		_, err := ParseSenderPolicy("")
		require.ErrorContains(t, err, "must not be empty")
	})

	t.Run("rejects invalid wildcard mix", func(t *testing.T) {
		_, err := ParseSenderPolicy("*,0x1111111111111111111111111111111111111111")
		require.ErrorContains(t, err, "must be used by itself")
	})

	t.Run("rejects invalid address", func(t *testing.T) {
		_, err := ParseSenderPolicy("not-an-address")
		require.ErrorContains(t, err, "invalid sender address")
	})

	t.Run("rejects empty entry", func(t *testing.T) {
		_, err := ParseSenderPolicy("0x1111111111111111111111111111111111111111,")
		require.ErrorContains(t, err, "empty entry")
	})
}

func TestConfigCheck_RequiresAllowedSendersWithoutPassthrough(t *testing.T) {
	cfg := &Config{Passthrough: false}
	require.ErrorContains(t, cfg.Check(), "allowed-senders must be configured")
}

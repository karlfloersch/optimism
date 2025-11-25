package flags

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseL2RPCs(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    []L2RPC
		wantErr string
	}{
		{
			name:  "single RPC",
			input: "10:http://localhost:8545",
			want: []L2RPC{
				{ChainID: 10, RPCURL: "http://localhost:8545"},
			},
		},
		{
			name:  "multiple RPCs",
			input: "10:http://op-mainnet:8545,8453:http://base:8545",
			want: []L2RPC{
				{ChainID: 10, RPCURL: "http://op-mainnet:8545"},
				{ChainID: 8453, RPCURL: "http://base:8545"},
			},
		},
		{
			name:  "RPC with port in URL",
			input: "420:http://localhost:9545",
			want: []L2RPC{
				{ChainID: 420, RPCURL: "http://localhost:9545"},
			},
		},
		{
			name:  "whitespace trimmed",
			input: " 10:http://localhost:8545 , 8453:http://base:8545 ",
			want: []L2RPC{
				{ChainID: 10, RPCURL: "http://localhost:8545"},
				{ChainID: 8453, RPCURL: "http://base:8545"},
			},
		},
		{
			name:    "empty string",
			input:   "",
			wantErr: "empty l2-rpcs",
		},
		{
			name:    "no colon at all",
			input:   "nocolon",
			wantErr: "invalid format",
		},
		{
			name:    "invalid chain ID",
			input:   "abc:http://localhost:8545",
			wantErr: "invalid chain ID",
		},
		{
			name:    "empty URL",
			input:   "10:",
			wantErr: "empty RPC URL",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseL2RPCs(tt.input)
			if tt.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

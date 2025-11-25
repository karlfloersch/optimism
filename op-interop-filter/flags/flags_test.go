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
			name:  "single RPC with chain ID",
			input: "10:http://localhost:8545",
			want: []L2RPC{
				{ChainID: 10, RPCURL: "http://localhost:8545"},
			},
		},
		{
			name:  "multiple RPCs with chain IDs",
			input: "10:http://op-mainnet:8545,8453:http://base:8545",
			want: []L2RPC{
				{ChainID: 10, RPCURL: "http://op-mainnet:8545"},
				{ChainID: 8453, RPCURL: "http://base:8545"},
			},
		},
		{
			name:  "chain name from superchain registry",
			input: "op-mainnet:http://localhost:8545",
			want: []L2RPC{
				{ChainID: 10, RPCURL: "http://localhost:8545"},
			},
		},
		{
			name:  "chain name op-sepolia",
			input: "op-sepolia:http://localhost:8545",
			want: []L2RPC{
				{ChainID: 11155420, RPCURL: "http://localhost:8545"},
			},
		},
		{
			name:  "chain name base-mainnet",
			input: "base-mainnet:http://localhost:8545",
			want: []L2RPC{
				{ChainID: 8453, RPCURL: "http://localhost:8545"},
			},
		},
		{
			name:  "mixed chain IDs and names",
			input: "op-mainnet:http://op:8545,8453:http://base:8545",
			want: []L2RPC{
				{ChainID: 10, RPCURL: "http://op:8545"},
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
			name:    "unknown chain name",
			input:   "unknown-chain:http://localhost:8545",
			wantErr: "unknown chain",
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

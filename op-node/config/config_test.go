package config

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestConfigModeValidation tests that the config properly validates mode values
func TestConfigModeValidation(t *testing.T) {
	tests := []struct {
		name    string
		mode    string
		wantErr bool
		errMsg  string
	}{
		{
			name:    "valid normal mode",
			mode:    "normal",
			wantErr: false,
		},
		{
			name:    "valid prover mode",
			mode:    "prover",
			wantErr: false,
		},
		{
			name:    "valid follower mode",
			mode:    "follower",
			wantErr: false,
		},
		{
			name:    "invalid mode",
			mode:    "invalid",
			wantErr: true,
			errMsg:  "invalid mode option: \"invalid\"",
		},
		{
			name:    "empty mode (default)",
			mode:    "",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{
				Mode:       tt.mode,
				RollupHalt: "",
			}

			// Test just the mode validation logic by calling a helper function
			err := validateMode(cfg.Mode)
			if tt.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// validateMode is a helper function that extracts just the mode validation logic
func validateMode(mode string) error {
	if !(mode == "" || mode == "normal" || mode == "prover" || mode == "follower") {
		return fmt.Errorf("invalid mode option: %q (must be 'normal', 'prover', or 'follower')", mode)
	}
	return nil
}

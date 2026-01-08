package filter

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/stretchr/testify/require"

	oprpc "github.com/ethereum-optimism/optimism/op-service/rpc"
	"github.com/ethereum-optimism/optimism/op-service/testlog"
)

// generateJWT creates a valid JWT token for testing
func generateJWT(secret []byte) string {
	return generateJWTWithTime(secret, time.Now().Unix())
}

// generateJWTWithTime creates a JWT token with a custom iat timestamp
func generateJWTWithTime(secret []byte, iat int64) string {
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	claims := fmt.Sprintf(`{"iat":%d}`, iat)
	payload := base64.RawURLEncoding.EncodeToString([]byte(claims))

	message := header + "." + payload
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(message))
	signature := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))

	return message + "." + signature
}

// callRPC makes an RPC call and returns whether it succeeded auth-wise
func callRPC(t *testing.T, endpoint, method string, token string) (authSucceeded bool, rpcError string) {
	reqBody := map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  method,
		"params":  []interface{}{},
		"id":      1,
	}
	body, err := json.Marshal(reqBody)
	require.NoError(t, err)

	req, err := http.NewRequest("POST", endpoint, bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	// 401/403 means auth failed
	if resp.StatusCode == 401 || resp.StatusCode == 403 {
		return false, fmt.Sprintf("HTTP %d: %s", resp.StatusCode, string(respBody))
	}

	// Parse response to check for RPC-level errors
	var result map[string]interface{}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return true, fmt.Sprintf("invalid JSON: %s", string(respBody))
	}

	if errObj, ok := result["error"]; ok {
		return true, fmt.Sprintf("RPC error: %v", errObj)
	}

	return true, ""
}

// mockL2Server creates a mock L2 RPC server for testing
func mockL2Server(t *testing.T) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req map[string]interface{}
		_ = json.Unmarshal(body, &req)

		method, _ := req["method"].(string)
		id := req["id"]

		var result interface{}
		switch method {
		case "eth_chainId":
			result = "0x1"
		case "eth_blockNumber":
			result = "0x100"
		case "eth_getBlockByNumber":
			result = map[string]interface{}{
				"number":     "0x100",
				"hash":       "0x" + "00"[0:1] + string(make([]byte, 63)),
				"parentHash": "0x" + "00"[0:1] + string(make([]byte, 63)),
				"timestamp":  "0x60000000",
			}
		default:
			result = nil
		}

		resp := map[string]interface{}{
			"jsonrpc": "2.0",
			"id":      id,
			"result":  result,
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
}

// TestJWTAuthenticationBehavior tests the expected JWT authentication behavior:
// - supervisor API should be PUBLIC (no auth required) - served on root /
// - admin API should REQUIRE JWT when admin is enabled - served on /admin
func TestJWTAuthenticationBehavior(t *testing.T) {
	logger := testlog.Logger(t, log.LevelInfo)
	ctx := context.Background()

	// Create mock L2 server
	mockL2 := mockL2Server(t)
	defer mockL2.Close()

	// Create JWT secret file
	jwtPath := filepath.Join(t.TempDir(), "jwt.txt")
	secret, err := oprpc.ObtainJWTSecret(logger, jwtPath, true)
	require.NoError(t, err)

	token := generateJWT(secret[:])

	t.Run("with_jwt_and_admin_enabled", func(t *testing.T) {
		cfg := &Config{
			L2RPCs:        []string{mockL2.URL},
			JWTSecretPath: jwtPath,
			RPC: oprpc.CLIConfig{
				ListenAddr:  "127.0.0.1",
				ListenPort:  0, // random port
				EnableAdmin: true,
			},
			MessageExpiryWindow: uint64(DefaultMessageExpiryWindow.Seconds()),
			PollInterval:        2 * time.Second,
			ValidationInterval:  500 * time.Millisecond,
		}

		svc, err := NewService(ctx, cfg, logger)
		require.NoError(t, err)
		require.NoError(t, svc.Start(ctx))
		defer func() { _ = svc.Stop(ctx) }()

		baseEndpoint := "http://" + svc.rpcServer.Endpoint()
		adminEndpoint := baseEndpoint + "/admin"
		t.Logf("Filter service running at %s (admin at %s)", baseEndpoint, adminEndpoint)

		// Wait for service to be ready
		time.Sleep(100 * time.Millisecond)

		// Test cases - note: admin API is on /admin route, supervisor on root /
		tests := []struct {
			name     string
			endpoint string
			method   string
			useToken bool
			wantAuth bool // true = auth should succeed, false = auth should fail
			desc     string
		}{
			{
				name:     "supervisor_without_token",
				endpoint: baseEndpoint,
				method:   "supervisor_checkAccessList",
				useToken: false,
				wantAuth: true, // supervisor should be PUBLIC
				desc:     "supervisor API should work WITHOUT token (root route is public)",
			},
			{
				name:     "supervisor_with_token",
				endpoint: baseEndpoint,
				method:   "supervisor_checkAccessList",
				useToken: true,
				wantAuth: true,
				desc:     "supervisor API should work WITH token",
			},
			{
				name:     "admin_without_token",
				endpoint: adminEndpoint,
				method:   "admin_getFailsafeEnabled",
				useToken: false,
				wantAuth: false, // admin MUST require auth
				desc:     "admin API should FAIL without token (/admin route requires JWT)",
			},
			{
				name:     "admin_with_token",
				endpoint: adminEndpoint,
				method:   "admin_getFailsafeEnabled",
				useToken: true,
				wantAuth: true,
				desc:     "admin API should work WITH token",
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				tokenToUse := ""
				if tc.useToken {
					tokenToUse = token
				}

				authOK, rpcErr := callRPC(t, tc.endpoint, tc.method, tokenToUse)

				if tc.wantAuth {
					if !authOK {
						t.Errorf("%s: expected auth to SUCCEED but got auth failure", tc.desc)
					} else {
						t.Logf("PASS: %s (auth succeeded, rpc_result: %s)", tc.desc, rpcErr)
					}
				} else {
					if authOK {
						t.Errorf("%s: expected auth to FAIL but it succeeded (rpc_result: %s)", tc.desc, rpcErr)
					} else {
						t.Logf("PASS: %s (auth correctly failed)", tc.desc)
					}
				}
			})
		}

		// NEGATIVE TESTS: Verify JWT validation actually works
		// These tests ensure we're not just checking token presence but actually validating

		t.Run("admin_with_wrong_secret", func(t *testing.T) {
			// Generate token with a DIFFERENT secret - should be rejected
			wrongSecret := make([]byte, 32)
			for i := range wrongSecret {
				wrongSecret[i] = byte(i) // deterministic but different from real secret
			}
			wrongToken := generateJWT(wrongSecret)

			authOK, _ := callRPC(t, adminEndpoint, "admin_getFailsafeEnabled", wrongToken)
			if authOK {
				t.Error("admin API should REJECT token signed with wrong secret")
			} else {
				t.Log("PASS: admin API correctly rejected token with wrong secret")
			}
		})

		t.Run("admin_with_expired_token", func(t *testing.T) {
			// Generate token with iat > 60 seconds in the past (go-ethereum rejects these)
			expiredToken := generateJWTWithTime(secret[:], time.Now().Unix()-120) // 2 minutes ago

			authOK, _ := callRPC(t, adminEndpoint, "admin_getFailsafeEnabled", expiredToken)
			if authOK {
				t.Error("admin API should REJECT expired token (iat > 60s ago)")
			} else {
				t.Log("PASS: admin API correctly rejected expired token")
			}
		})

		t.Run("admin_with_future_token", func(t *testing.T) {
			// Generate token with iat > 60 seconds in the future
			futureToken := generateJWTWithTime(secret[:], time.Now().Unix()+120) // 2 minutes in future

			authOK, _ := callRPC(t, adminEndpoint, "admin_getFailsafeEnabled", futureToken)
			if authOK {
				t.Error("admin API should REJECT future token (iat > 60s in future)")
			} else {
				t.Log("PASS: admin API correctly rejected future token")
			}
		})

		t.Run("admin_with_malformed_token", func(t *testing.T) {
			// Send garbage that looks nothing like a JWT
			malformedToken := "not.a.valid.jwt.token"

			authOK, _ := callRPC(t, adminEndpoint, "admin_getFailsafeEnabled", malformedToken)
			if authOK {
				t.Error("admin API should REJECT malformed token")
			} else {
				t.Log("PASS: admin API correctly rejected malformed token")
			}
		})

		t.Run("admin_with_empty_token", func(t *testing.T) {
			// Send empty Authorization header value
			authOK, _ := callRPC(t, adminEndpoint, "admin_getFailsafeEnabled", "")

			if authOK {
				t.Error("admin API should REJECT empty token")
			} else {
				t.Log("PASS: admin API correctly rejected empty token")
			}
		})
	})

	t.Run("without_jwt_admin_disabled", func(t *testing.T) {
		cfg := &Config{
			L2RPCs: []string{mockL2.URL},
			// No JWT secret path
			RPC: oprpc.CLIConfig{
				ListenAddr:  "127.0.0.1",
				ListenPort:  0,
				EnableAdmin: false, // admin disabled
			},
			MessageExpiryWindow: uint64(DefaultMessageExpiryWindow.Seconds()),
			PollInterval:        2 * time.Second,
			ValidationInterval:  500 * time.Millisecond,
		}

		svc, err := NewService(ctx, cfg, logger)
		require.NoError(t, err)
		require.NoError(t, svc.Start(ctx))
		defer func() { _ = svc.Stop(ctx) }()

		endpoint := "http://" + svc.rpcServer.Endpoint()
		time.Sleep(100 * time.Millisecond)

		// supervisor should work without any auth
		authOK, rpcErr := callRPC(t, endpoint, "supervisor_checkAccessList", "")
		if !authOK {
			t.Errorf("supervisor API should be public when no JWT configured, got auth failure")
		} else {
			t.Logf("PASS: supervisor API is public (rpc_result: %s)", rpcErr)
		}

		// admin should not be available (disabled)
		authOK, rpcErr = callRPC(t, endpoint, "admin_getFailsafeEnabled", "")
		// This should fail with "method not found" not auth error
		if authOK && rpcErr != "" {
			t.Logf("PASS: admin API not available when disabled (error: %s)", rpcErr)
		}
	})
}

package supervisor

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/types"
	"github.com/ethereum/go-ethereum/common"
)

func TestDenylistHTTP(t *testing.T) {
	sup := NewSupervisor(testLogger())
	srv := httptest.NewServer(sup.HTTPHandler())
	defer srv.Close()

	// check default denylist false
	resp, err := http.Get(srv.URL + "/denylist/v1/check?chainId=901&id=0xabc")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	if v, _ := out["denylisted"].(bool); v {
		t.Fatalf("unexpected denylisted true")
	}

	// emulate internal add
	_ = sup.denylist.Add(901, "0xabc")

	// check again, expect true
	resp, err = http.Get(srv.URL + "/denylist/v1/check?chainId=901&id=0xabc")
	if err != nil {
		t.Fatalf("get2: %v", err)
	}
	defer resp.Body.Close()
	out = map[string]any{}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	if v, _ := out["denylisted"].(bool); !v {
		t.Fatalf("expected denylisted true")
	}
}

func TestFinalityAuthorizationHTTP(t *testing.T) {
	sup := NewSupervisor(testLogger())
	srv := httptest.NewServer(sup.HTTPHandler())
	defer srv.Close()

	// Test 1: No cross-safe history - should deny finality
	resp, err := http.Get(srv.URL + "/authorize_finality/v1/check?timestamp=1000")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	if v, _ := out["authorized"].(bool); v {
		t.Fatalf("expected authorization false with no cross-safe history")
	}

	// Test 2: Add cross-safe history entry
	sup.mu.Lock()
	sup.crossSafeHistory = []crossSafeMD{
		{
			Timestamp: 2000,
			L1Block:   eth.BlockRef{Number: 50, Hash: common.HexToHash("0x123")},
			L2Blocks: map[uint64]types.DerivedBlockRefPair{
				901: {
					Derived: eth.BlockRef{
						Number: 150,
						Hash:   common.HexToHash("0xdef"),
						Time:   2000,
					},
					Source: eth.BlockRef{
						Number: 50,
						Hash:   common.HexToHash("0x123"),
						Time:   2000,
					},
				},
			},
		},
	}
	sup.mu.Unlock()

	// Test 3: Valid finality request (timestamp <= cross-safe timestamp)
	resp, err = http.Get(srv.URL + "/authorize_finality/v1/check?timestamp=2000")
	if err != nil {
		t.Fatalf("get valid: %v", err)
	}
	defer resp.Body.Close()
	out = map[string]any{}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	if v, _ := out["authorized"].(bool); !v {
		t.Fatalf("expected authorization true for valid timestamp")
	}

	// Test 4: Valid finality request (timestamp < cross-safe timestamp)
	resp, err = http.Get(srv.URL + "/authorize_finality/v1/check?timestamp=1500")
	if err != nil {
		t.Fatalf("get valid older: %v", err)
	}
	defer resp.Body.Close()
	out = map[string]any{}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	if v, _ := out["authorized"].(bool); !v {
		t.Fatalf("expected authorization true for older timestamp")
	}

	// Test 5: Invalid timestamp (too recent)
	resp, err = http.Get(srv.URL + "/authorize_finality/v1/check?timestamp=3000")
	if err != nil {
		t.Fatalf("get timestamp invalid: %v", err)
	}
	defer resp.Body.Close()
	out = map[string]any{}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	if v, _ := out["authorized"].(bool); v {
		t.Fatalf("expected authorization false for timestamp too recent")
	}
}

// uses package-level testLogger from testutil_logger.go

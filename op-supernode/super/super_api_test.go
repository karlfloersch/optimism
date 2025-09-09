package super

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestDenylistHTTP(t *testing.T) {
	sup := NewSuper(testLogger())
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

	// emulate internal add via cross service
	if sup.crossService != nil {
		_ = sup.crossService.AddToDenylist(901, 1234567890, "0xabc")
	}

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
	sup := NewSuper(testLogger())
	srv := httptest.NewServer(sup.HTTPHandler())
	defer srv.Close()

	// Test finality authorization endpoint - currently always returns true
	// TODO: Update when cross service provides proper authorization
	resp, err := http.Get(srv.URL + "/authorize_finality/v1/check?timestamp=1000")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)

	// Currently the API returns true as authorization is delegated to cross service
	if v, _ := out["authorized"].(bool); !v {
		t.Fatalf("expected authorization true (current implementation)")
	}
}

// uses package-level testLogger from testutil_logger.go

package supervisor

import (
    "encoding/json"
    "net/http"
    "net/http/httptest"
    "testing"
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

// uses package-level testLogger from testutil_logger.go

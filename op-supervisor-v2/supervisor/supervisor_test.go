package supervisor

import (
    "encoding/json"
    "net/http"
    "net/http/httptest"
    "testing"

    oplog "github.com/ethereum-optimism/optimism/op-service/log"
)

func newTestSupervisor(t *testing.T) *Supervisor {
    t.Helper()
    lgr := oplog.NewLogger(nil, oplog.DefaultCLIConfig())
    return NewSupervisor(lgr)
}

func TestHTTPHealthAndStatus(t *testing.T) {
    sup := newTestSupervisor(t)
    srv := httptest.NewServer(sup.HTTPHandler())
    defer srv.Close()

    // healthz
    resp, err := http.Get(srv.URL + "/healthz")
    if err != nil {
        t.Fatalf("GET /healthz: %v", err)
    }
    if resp.StatusCode != 200 {
        t.Fatalf("expected 200, got %d", resp.StatusCode)
    }

    // status
    resp, err = http.Get(srv.URL + "/status")
    if err != nil {
        t.Fatalf("GET /status: %v", err)
    }
    defer resp.Body.Close()
    var body map[string]any
    if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
        t.Fatalf("decode status: %v", err)
    }
    if _, ok := body["op_node_running"]; !ok {
        t.Fatalf("status missing op_node_running")
    }
}

func TestStopWithoutStart(t *testing.T) {
    sup := newTestSupervisor(t)
    // Should not panic
    sup.Stop()
}
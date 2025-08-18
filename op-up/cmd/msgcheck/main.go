package main

import (
    "bufio"
    "encoding/json"
    "errors"
    "flag"
    "fmt"
    "net/http"
    "os"
    "path/filepath"
    "regexp"
    "strings"
    "time"
)

type syncStatus struct {
    Chains map[string]struct {
        LocalUnsafe blockID `json:"LocalUnsafe"`
        LocalSafe   blockID `json:"LocalSafe"`
        CrossSafe   blockID `json:"CrossSafe"`
    } `json:"Chains"`
}

type blockID struct {
    Hash   string `json:"Hash"`
    Number uint64 `json:"Number"`
}

func main() {
    var (
        envFile      = flag.String("env-file", "op-up/external-l1.env", "Path to Sepolia external L1 env file")
        mode         = flag.String("mode", "both", "Run mode: valid|invalid|both")
        timeout      = flag.Duration("timeout", 10*time.Minute, "Overall timeout")
        pollInterval = flag.Duration("poll-interval", 250*time.Millisecond, "Polling interval")
        logFile      = flag.String("log-file", "", "Optional log output file (append)")
    )
    flag.Parse()

    if *logFile != "" {
        f, err := os.OpenFile(*logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
        if err == nil {
            _ = os.Stdout.Sync()
            _ = os.Stderr.Sync()
            os.Stdout = f
            os.Stderr = f
        }
    }

    // Parse minimal env we might need (no strict requirements yet)
    env, _ := parseExportEnvFile(*envFile)
    _ = env // reserved for future use (keys, RPCs)

    sv2URL, err := discoverSV2URL()
    if err != nil {
        fmt.Printf("ERROR: discover SV2 URL: %v\n", err)
        os.Exit(1)
    }
    fmt.Printf("SV2 URL: %s\n", sv2URL)

    ctxDeadline := time.Now().Add(*timeout)
    if err := waitSV2Advancing(sv2URL, []string{"901", "902"}, ctxDeadline, *pollInterval); err != nil {
        fmt.Printf("ERROR: SV2 readiness: %v\n", err)
        os.Exit(1)
    }
    fmt.Println("SV2 readiness OK: chains 901 and 902 advancing")

    switch strings.ToLower(*mode) {
    case "valid":
        fmt.Println("[stub] submitting VALID executing message (to be implemented)")
    case "invalid":
        fmt.Println("[stub] submitting INVALID executing message (to be implemented)")
    case "both":
        fmt.Println("[stub] submitting VALID executing message (to be implemented)")
        fmt.Println("[stub] submitting INVALID executing message (to be implemented)")
    default:
        fmt.Printf("unknown mode: %s\n", *mode)
        os.Exit(1)
    }
}

func parseExportEnvFile(path string) (map[string]string, error) {
    f, err := os.Open(path)
    if err != nil {
        return nil, err
    }
    defer f.Close()
    out := make(map[string]string)
    re := regexp.MustCompile(`^\s*export\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*"?(.*?)"?\s*$`)
    s := bufio.NewScanner(f)
    for s.Scan() {
        line := strings.TrimSpace(s.Text())
        if strings.HasPrefix(line, "#") || line == "" { continue }
        if m := re.FindStringSubmatch(line); m != nil {
            key, val := m[1], m[2]
            out[key] = val
        }
    }
    return out, nil
}

func discoverSV2URL() (string, error) {
    // Prefer latest log file under op-up/logs, search for line: "[sv2] http: <url>"
    logsDir := filepath.Join("op-up", "logs")
    entries, err := os.ReadDir(logsDir)
    if err != nil {
        return "", err
    }
    var newest string
    var newestMod time.Time
    for _, e := range entries {
        if e.IsDir() { continue }
        name := e.Name()
        if !strings.HasPrefix(name, "op-up.") { continue }
        info, _ := e.Info()
        if info != nil && info.ModTime().After(newestMod) {
            newestMod = info.ModTime()
            newest = filepath.Join(logsDir, name)
        }
    }
    if newest == "" {
        // fallback to known latest symlink if present
        newest = filepath.Join(logsDir, "op-up.latest.log")
    }
    f, err := os.Open(newest)
    if err != nil {
        return "", err
    }
    defer f.Close()
    var sv2 string
    s := bufio.NewScanner(f)
    for s.Scan() {
        line := s.Text()
        if i := strings.Index(line, "[sv2] http:"); i >= 0 {
            // expect: [sv2] http: http://127.0.0.1:PORT
            parts := strings.Fields(line)
            for _, p := range parts {
                if strings.HasPrefix(p, "http://") || strings.HasPrefix(p, "https://") {
                    sv2 = strings.TrimSpace(p)
                }
            }
        }
    }
    if sv2 == "" {
        return "", errors.New("could not find SV2 URL in logs")
    }
    return sv2, nil
}

func waitSV2Advancing(sv2URL string, chainIDs []string, deadline time.Time, interval time.Duration) error {
    type chainHead struct { num uint64 }
    last := map[string]chainHead{}
    client := &http.Client{ Timeout: 3 * time.Second }
    for time.Now().Before(deadline) {
        req, _ := http.NewRequest(http.MethodGet, sv2URL+"/v1/sync_status", nil)
        resp, err := client.Do(req)
        if err == nil && resp != nil && resp.Body != nil {
            var st syncStatus
            _ = json.NewDecoder(resp.Body).Decode(&st)
            resp.Body.Close()
            ok := true
            for _, id := range chainIDs {
                ch, exists := st.Chains[id]
                if !exists || ch.LocalUnsafe.Number == 0 {
                    ok = false
                    break
                }
                prev := last[id]
                if ch.LocalUnsafe.Number <= prev.num {
                    ok = false
                }
                last[id] = chainHead{num: ch.LocalUnsafe.Number}
            }
            if ok { return nil }
        }
        time.Sleep(interval)
    }
    return fmt.Errorf("timeout waiting for SV2 to advance chains: %v", chainIDs)
}



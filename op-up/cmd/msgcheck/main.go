package main

import (
    "context"
    "bufio"
    "encoding/json"
    "errors"
    "flag"
    "fmt"
    "math/big"
    "net/http"
    "os"
    "path/filepath"
    "regexp"
    "strings"
    "time"

    "crypto/ecdsa"
    "github.com/ethereum/go-ethereum/core/types"
    "github.com/ethereum/go-ethereum/crypto"
    "github.com/ethereum/go-ethereum/ethclient"
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
        rpc901       = flag.String("rpc-901", "http://127.0.0.1:9545", "Execution RPC URL for chain 901")
        rpc902       = flag.String("rpc-902", "http://127.0.0.1:9546", "Execution RPC URL for chain 902")
        sv2Flag      = flag.String("sv2-url", "", "Optional SV2 base URL (skip discovery if set)")
        privKeyFlag  = flag.String("priv-key", "", "Private key for sending txs (0x...) - optional if present in env file as FAUCET_PK")
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

    sv2URL := strings.TrimSpace(*sv2Flag)
    if sv2URL == "" {
        var err error
        sv2URL, err = discoverSV2URL()
        if err != nil {
            fmt.Printf("WARN: SV2 URL discover failed: %v\n", err)
        }
    }
    if sv2URL != "" {
        fmt.Printf("SV2 URL: %s\n", sv2URL)
        ctxDeadline := time.Now().Add(*timeout)
        if err := waitSV2Advancing(sv2URL, []string{"901", "902"}, ctxDeadline, *pollInterval); err != nil {
            fmt.Printf("ERROR: SV2 readiness: %v\n", err)
            os.Exit(1)
        }
        fmt.Println("SV2 readiness OK: chains 901 and 902 advancing")
    } else {
        // Fallback: small delay before tx attempts
        time.Sleep(2 * time.Second)
    }

    // Determine private key
    privKey := strings.TrimSpace(*privKeyFlag)
    if privKey == "" {
        if v, ok := env["FAUCET_PK"]; ok {
            privKey = strings.TrimSpace(v)
        }
    }
    if !strings.HasPrefix(privKey, "0x") && privKey != "" {
        privKey = "0x" + privKey
    }

    switch strings.ToLower(*mode) {
    case "valid":
        if err := sendSimpleTxBoth([]string{*rpc901, *rpc902}, privKey, *timeout); err != nil {
            fmt.Printf("ERROR: send valid txs: %v\n", err)
            os.Exit(1)
        }
    case "invalid":
        fmt.Println("[stub] submitting INVALID executing message (to be implemented)")
    case "both":
        if err := sendSimpleTxBoth([]string{*rpc901, *rpc902}, privKey, *timeout); err != nil {
            fmt.Printf("ERROR: send valid txs: %v\n", err)
            os.Exit(1)
        }
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

// sendSimpleTxBoth sends a 0 ETH self-transfer on each provided RPC URL, using the given private key
// If privKey is empty, a random key is used and the tx may fail due to insufficient funds.
func sendSimpleTxBoth(rpcs []string, privKey string, overallTimeout time.Duration) error {
    if len(rpcs) == 0 { return nil }
    for _, url := range rpcs {
        if strings.TrimSpace(url) == "" { continue }
        if err := sendSimpleTx(url, privKey, overallTimeout); err != nil {
            return fmt.Errorf("rpc %s: %w", url, err)
        }
    }
    return nil
}

func sendSimpleTx(rpcURL, privKey string, overallTimeout time.Duration) error {
    ctx, cancel := contextWithTimeout(overallTimeout)
    defer cancel()

    cli, err := ethclient.DialContext(ctx, rpcURL)
    if err != nil { return err }
    defer cli.Close()

    // Use ECDSA key from geth crypto
    var pkObj *ecdsa.PrivateKey
    if privKey != "" {
        sk := strings.TrimPrefix(privKey, "0x")
        if pk, err := crypto.HexToECDSA(sk); err == nil {
            pkObj = pk
        } else {
            return fmt.Errorf("parse key: %w", err)
        }
    } else {
        // Generate ephemeral unfunded key (tx may fail); prefer caller-provided key
        tmp, err := crypto.GenerateKey()
        if err != nil { return err }
        pkObj = tmp
    }

    from := crypto.PubkeyToAddress(pkObj.PublicKey)
    to := from

    chainID, err := cli.ChainID(ctx)
    if err != nil { return err }
    nonce, err := cli.PendingNonceAt(ctx, from)
    if err != nil { return err }
    tipCap, err := cli.SuggestGasTipCap(ctx)
    if err != nil { return err }
    header, err := cli.HeaderByNumber(ctx, nil)
    if err != nil { return err }
    base := header.BaseFee
    if base == nil { base = big.NewInt(0) }
    feeCap := new(big.Int).Add(new(big.Int).Mul(base, big.NewInt(2)), tipCap)

    gas := uint64(21000)
    tx := types.NewTx(&types.DynamicFeeTx{
        ChainID:   chainID,
        Nonce:     nonce,
        GasTipCap: tipCap,
        GasFeeCap: feeCap,
        Gas:       gas,
        To:        &to,
        Value:     big.NewInt(0),
        Data:      nil,
    })
    stx, err := types.SignTx(tx, types.NewLondonSigner(chainID), pkObj)
    if err != nil { return err }
    if err := cli.SendTransaction(ctx, stx); err != nil { return err }

    // Wait for receipt
    deadline := time.Now().Add(overallTimeout)
    for time.Now().Before(deadline) {
        r, err := cli.TransactionReceipt(context.Background(), stx.Hash())
        if err == nil && r != nil && r.BlockNumber != nil {
            fmt.Printf("tx %s confirmed on %s (status=%d)\n", stx.Hash().Hex(), rpcURL, r.Status)
            return nil
        }
        time.Sleep(500 * time.Millisecond)
    }
    return fmt.Errorf("timeout waiting for receipt: %s", stx.Hash())
}

// tiny helper to create a cancellable context with default if zero
func contextWithTimeout(d time.Duration) (context.Context, context.CancelFunc) {
    if d <= 0 { d = 30 * time.Second }
    return context.WithTimeout(context.Background(), d)
}



package main

import (
    "context"
    "crypto/ecdsa"
    "errors"
    "flag"
    "fmt"
    "log"
    "math/big"
    "strings"
    "time"

    "github.com/ethereum/go-ethereum/common"
    "github.com/ethereum/go-ethereum/core/types"
    "github.com/ethereum/go-ethereum/crypto"
    "github.com/ethereum/go-ethereum/ethclient"
)

func main() {
    var (
        rpcURL   = flag.String("rpc-url", "http://127.0.0.1:9545", "Execution JSON-RPC URL")
        privKey  = flag.String("priv-key", "", "EOA private key (0x...)")
        toStr    = flag.String("to", "", "Recipient address (default: self)")
        valueEth = flag.Float64("value", 0, "Value in ETH (default: 0)")
        timeout  = flag.Duration("timeout", 30*time.Second, "Overall timeout")
    )
    flag.Parse()

    if *privKey == "" {
        log.Fatal("--priv-key required")
    }
    pk, err := parsePrivateKey(*privKey)
    if err != nil {
        log.Fatalf("invalid priv key: %v", err)
    }
    from := crypto.PubkeyToAddress(pk.PublicKey)
    to := from
    if *toStr != "" {
        to = common.HexToAddress(*toStr)
    }

    ctx, cancel := context.WithTimeout(context.Background(), *timeout)
    defer cancel()

    cli, err := ethclient.DialContext(ctx, *rpcURL)
    if err != nil {
        log.Fatalf("dial rpc: %v", err)
    }
    defer cli.Close()

    chainID, err := cli.ChainID(ctx)
    if err != nil {
        log.Fatalf("chain id: %v", err)
    }

    // Nonce
    nonce, err := cli.PendingNonceAt(ctx, from)
    if err != nil {
        log.Fatalf("nonce: %v", err)
    }

    // Fees (EIP-1559)
    tipCap, err := cli.SuggestGasTipCap(ctx)
    if err != nil {
        log.Fatalf("tip cap: %v", err)
    }
    header, err := cli.HeaderByNumber(ctx, nil)
    if err != nil {
        log.Fatalf("header: %v", err)
    }
    baseFee := header.BaseFee
    if baseFee == nil {
        baseFee = big.NewInt(0)
    }
    // GasFeeCap = baseFee*2 + tip
    feeCap := new(big.Int).Add(new(big.Int).Mul(baseFee, big.NewInt(2)), tipCap)

    // Value
    wei := ethToWei(*valueEth)

    // Gas limit for simple native transfer
    gasLimit := uint64(21000)

    tx := types.NewTx(&types.DynamicFeeTx{
        ChainID:   chainID,
        Nonce:     nonce,
        GasTipCap: tipCap,
        GasFeeCap: feeCap,
        Gas:       gasLimit,
        To:        &to,
        Value:     wei,
        Data:      nil,
    })
    signer := types.NewLondonSigner(chainID)
    stx, err := types.SignTx(tx, signer, pk)
    if err != nil {
        log.Fatalf("sign: %v", err)
    }
    if err := cli.SendTransaction(ctx, stx); err != nil {
        log.Fatalf("send: %v", err)
    }
    fmt.Printf("tx: %s\n", stx.Hash().Hex())

    // Wait for receipt
    receiptCtx, cancel2 := context.WithTimeout(context.Background(), 60*time.Second)
    defer cancel2()
    for receiptCtx.Err() == nil {
        r, err := cli.TransactionReceipt(receiptCtx, stx.Hash())
        if err == nil && r != nil && r.BlockNumber != nil {
            fmt.Printf("receipt: status=%d block=%s\n", r.Status, r.BlockNumber.String())
            return
        }
        time.Sleep(500 * time.Millisecond)
    }
    log.Fatal("timeout waiting for receipt")
}

func parsePrivateKey(s string) (*ecdsa.PrivateKey, error) {
    s = strings.TrimSpace(s)
    if strings.HasPrefix(s, "0x") || strings.HasPrefix(s, "0X") {
        s = s[2:]
    }
    if s == "" {
        return nil, errors.New("empty key")
    }
    return crypto.HexToECDSA(s)
}

func ethToWei(v float64) *big.Int {
    // v ETH -> wei
    // Avoid float precision for non-zero values by simple scaling where possible
    // For our smoke use with 0 value, this is fine; otherwise scale by 1e18
    f := new(big.Float).SetFloat64(v)
    weiF := new(big.Float).Mul(f, new(big.Float).SetInt(big.NewInt(1_000_000_000_000_000_000)))
    wei := new(big.Int)
    weiF.Int(wei)
    return wei
}



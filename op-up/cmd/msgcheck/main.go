package main

import (
	"bufio"
	"context"
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

	"github.com/ethereum/go-ethereum/accounts/abi"

	"crypto/ecdsa"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"

	bindings "github.com/ethereum-optimism/optimism/devnet-sdk/contracts/bindings"
	inboxbinding "github.com/ethereum-optimism/optimism/op-e2e/e2eutils/contracts/bindings/inbox"

	// Added for access-list encoding
	opeth "github.com/ethereum-optimism/optimism/op-service/eth"
	suptypes "github.com/ethereum-optimism/optimism/op-supervisor/supervisor/types"
)

type syncStatus struct {
	Chains map[string]struct {
		LocalUnsafe blockID `json:"localUnsafe"`
		LocalSafe   blockID `json:"localSafe"`
		CrossSafe   blockID `json:"safe"`
		Finalized   blockID `json:"finalized"`
	} `json:"chains"`
}

type blockID struct {
	Hash   string `json:"hash"`
	Number uint64 `json:"number"`
}

// RelayArgsFile describes the inputs required to call CrossL2Inbox.validateMessage
// It intentionally contains only what the second transaction needs: destination RPC,
// the Identifier tuple, and the msgHash.
type RelayArgsFile struct {
	DstRPC     string          `json:"dst_rpc"`
	Identifier RelayIdentifier `json:"identifier"`
	MsgHash    string          `json:"msg_hash"` // 0x-hex
}

type RelayIdentifier struct {
	Origin      string `json:"origin"`      // 0x-addr
	BlockNumber string `json:"blockNumber"` // decimal string
	LogIndex    string `json:"logIndex"`    // decimal string
	Timestamp   string `json:"timestamp"`   // decimal string
	ChainID     string `json:"chainId"`     // decimal string (source chain id)
}

func writeRelayArgsFile(path string, dstRPC string, id [5]*big.Int, msgHash common.Hash) error {
	// Convert to user-friendly JSON
	out := RelayArgsFile{
		DstRPC: dstRPC,
		Identifier: RelayIdentifier{
			Origin:      common.BytesToAddress(id[0].Bytes()).Hex(),
			BlockNumber: id[1].String(),
			LogIndex:    id[2].String(),
			Timestamp:   id[3].String(),
			ChainID:     id[4].String(),
		},
		MsgHash: msgHash.Hex(),
	}
	b, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		return err
	}
	// Ensure directory exists
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}

func readRelayArgsFile(path string) (string, [5]*big.Int, common.Hash, error) {
	var zero [5]*big.Int
	b, err := os.ReadFile(path)
	if err != nil {
		return "", zero, common.Hash{}, err
	}
	var in RelayArgsFile
	if err := json.Unmarshal(b, &in); err != nil {
		return "", zero, common.Hash{}, err
	}
	// Parse identifier
	id := [5]*big.Int{new(big.Int), new(big.Int), new(big.Int), new(big.Int), new(big.Int)}
	id[0].SetBytes(common.HexToAddress(in.Identifier.Origin).Bytes())
	if _, ok := id[1].SetString(strings.TrimSpace(in.Identifier.BlockNumber), 10); !ok {
		return "", zero, common.Hash{}, fmt.Errorf("invalid blockNumber")
	}
	if _, ok := id[2].SetString(strings.TrimSpace(in.Identifier.LogIndex), 10); !ok {
		return "", zero, common.Hash{}, fmt.Errorf("invalid logIndex")
	}
	if _, ok := id[3].SetString(strings.TrimSpace(in.Identifier.Timestamp), 10); !ok {
		return "", zero, common.Hash{}, fmt.Errorf("invalid timestamp")
	}
	if _, ok := id[4].SetString(strings.TrimSpace(in.Identifier.ChainID), 10); !ok {
		return "", zero, common.Hash{}, fmt.Errorf("invalid chainId")
	}
	var mh common.Hash
	if len(in.MsgHash) > 0 {
		mh = common.HexToHash(in.MsgHash)
	}
	return strings.TrimSpace(in.DstRPC), id, mh, nil
}

func main() {
	var (
		envFile      = flag.String("env-file", "op-up/external-l1.env", "Path to Sepolia external L1 env file")
		mode         = flag.String("mode", "tx", "Run mode: tx|valid-msg|invalid-msg|heads|relay-file (aliases: valid->tx, invalid->invalid-msg, both->tx+invalid-msg)")
		timeout      = flag.Duration("timeout", 10*time.Minute, "Overall timeout")
		pollInterval = flag.Duration("poll-interval", 250*time.Millisecond, "Polling interval")
		logFile      = flag.String("log-file", "", "Optional log output file (append)")
		rpc901       = flag.String("rpc-901", "http://127.0.0.1:9545", "Execution RPC URL for chain 901")
		rpc902       = flag.String("rpc-902", "http://127.0.0.1:9546", "Execution RPC URL for chain 902")
		sv2Flag      = flag.String("sv2-url", "", "Optional SV2 base URL (skip discovery if set)")
		sv2Wait      = flag.Bool("sv2-wait", false, "If set, wait for SV2 to be advancing before proceeding")
		privKeyFlag  = flag.String("priv-key", "", "Private key for sending txs (0x...) - optional if present in env file as FAUCET_PK")
		fromChain    = flag.String("from", "901", "Source chain for L2->L2 message (901 or 902); destination is the other chain")
		targetFlag   = flag.String("target", "", "Optional target address for L2->L2 message (default: EOA self for valid-msg; CrossL2Inbox for invalid-msg)")
		// Simple tx batching (tx mode only)
		// (removed) repeat/repat flags
		// New flags for load testing
		saveArgsFile = flag.String("save-args-file", "", "If set, save CrossL2Inbox relay args (dst rpc, identifier, msgHash) to this JSON file and exit (valid-msg only)")
		argsFile     = flag.String("args-file", "", "Path to JSON file with CrossL2Inbox relay args (for mode=relay-file)")
		mutateChk    = flag.Bool("mutate-checksum", false, "If set in relay-file mode, mutate checksum inputs (logIndex) before sending")
		multFlag     = flag.Int("mult", 1, "If >1 and <=10, relay-file will submit that many identical txs with ascending nonces as a batch")
	)
	flag.Parse()

	// (removed) repeat normalization

	// Normalize mult for relay-file
	mult := *multFlag
	if mult < 1 {
		mult = 1
	}
	if mult > 10 {
		mult = 10
	}

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
		if u, err := discoverSV2URL(); err == nil {
			sv2URL = u
		}
	}
	if sv2URL != "" && *sv2Wait {
		fmt.Printf("SV2 URL: %s\n", sv2URL)
		ctxDeadline := time.Now().Add(*timeout)
		chainRPCs := map[string]string{"901": *rpc901, "902": *rpc902}
		if err := waitSV2Advancing(sv2URL, chainRPCs, ctxDeadline, *pollInterval); err != nil {
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

	modeLower := strings.ToLower(*mode)
	// Back-compat aliases
	if modeLower == "valid" {
		modeLower = "tx"
	}
	if modeLower == "invalid" {
		modeLower = "invalid-msg"
	}
	if modeLower == "both" {
		// tx then invalid-msg
		modeLower = "tx+invalid-msg"
	}

	switch modeLower {
	case "heads":
		if err := showHeads(sv2URL, *rpc901, *rpc902); err != nil {
			fmt.Printf("ERROR: heads: %v\n", err)
			os.Exit(1)
		}
		return
	case "tx":
		if err := sendSimpleTxBoth([]string{*rpc901, *rpc902}, privKey, *timeout); err != nil {
			fmt.Printf("ERROR: send txs: %v\n", err)
			os.Exit(1)
		}
	case "valid-msg":
		// Use messenger path like earlier: send initiating msg via L2ToL2CrossDomainMessenger, relay via CrossL2Inbox
		srcRPC, dstChain := pickSrcDst(*fromChain, *rpc901, *rpc902)
		// Determine destination RPC
		var dstRPCRelay string
		if dstChain == "901" {
			dstRPCRelay = *rpc901
		} else {
			dstRPCRelay = *rpc902
		}

		var target common.Address
		if *targetFlag != "" {
			target = common.HexToAddress(*targetFlag)
		} else {
			pkObj, err := parseECDSA(privKey)
			if err != nil {
				fmt.Printf("ERROR: parse key: %v\n", err)
				os.Exit(1)
			}
			target = crypto.PubkeyToAddress(pkObj.PublicKey)
		}
		receipt, err := sendL2ToL2Message(srcRPC, dstChain, privKey, target, []byte{})
		if err != nil {
			fmt.Printf("ERROR: send L2->L2 valid message: %v\n", err)
			os.Exit(1)
		}
		if receipt == nil || receipt.BlockNumber == nil {
			fmt.Println("ERROR: missing receipt or block number")
			os.Exit(1)
		}
		_, sentPayload, id, err := buildRelayFromReceipt(srcRPC, receipt)
		if err != nil {
			fmt.Printf("ERROR: build relay payload: %v\n", err)
			os.Exit(1)
		}
		// Optionally save args for second tx and exit
		if strings.TrimSpace(*saveArgsFile) != "" {
			msgHash := crypto.Keccak256Hash(sentPayload)
			if err := writeRelayArgsFile(*saveArgsFile, dstRPCRelay, id, msgHash); err != nil {
				fmt.Printf("ERROR: save args file: %v\n", err)
				os.Exit(1)
			}
			fmt.Printf("Saved relay args to %s (dst=%s)\n", *saveArgsFile, dstRPCRelay)
			return
		}
		recRelay, err := relayMessage(dstRPCRelay, privKey, id, sentPayload)
		if err != nil {
			fmt.Printf("ERROR: relayMessage: %v\n", err)
			os.Exit(1)
		}
		if recRelay == nil || recRelay.Status != types.ReceiptStatusSuccessful {
			fmt.Println("ERROR: relay receipt missing or non-success")
			os.Exit(1)
		}
		// Get block timestamp
		ctx, cancel := contextWithTimeout(5 * time.Second)
		cli, err := ethclient.DialContext(ctx, dstRPCRelay)
		var blockTimestamp uint64
		if err == nil {
			if header, err := cli.HeaderByNumber(ctx, recRelay.BlockNumber); err == nil {
				blockTimestamp = header.Time
			}
			cli.Close()
		}
		cancel()
		fmt.Printf("Valid Message: ok (block %d, timestamp %d)\n", recRelay.BlockNumber.Uint64(), blockTimestamp)
	case "relay-file":
		// Replay CrossL2Inbox.validateMessage from a saved args file
		if strings.TrimSpace(*argsFile) == "" {
			fmt.Println("ERROR: --args-file is required for mode=relay-file")
			os.Exit(1)
		}
		dstRPC, id, msgHash, err := readRelayArgsFile(*argsFile)
		if err != nil {
			fmt.Printf("ERROR: read args file: %v\n", err)
			os.Exit(1)
		}
		if *mutateChk {
			msgHash = common.HexToHash("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef")
		}
		if mult <= 1 {
			recRelay, err := relayMessageHash(dstRPC, privKey, id, msgHash)
			if err != nil {
				fmt.Printf("❌ (%v)\n", err)
				os.Exit(1)
			}
			fmt.Printf("✅ (block %d)\n", recRelay.BlockNumber.Uint64())
			break
		}
		// Batch submit 'mult' identical relay txs with ascending nonces
		if err := relayMessageHashBatch(dstRPC, privKey, id, msgHash, mult); err != nil {
			fmt.Printf("❌ (%v)\n", err)
			os.Exit(1)
		}
		fmt.Printf("✅ batch %d submitted\n", mult)
	case "invalid-msg":
		srcRPC, dstChain := pickSrcDst(*fromChain, *rpc901, *rpc902)
		// Determine destination RPC
		var dstRPCRelay string
		if dstChain == "901" {
			dstRPCRelay = *rpc901
		} else {
			dstRPCRelay = *rpc902
		}

		var target common.Address
		if *targetFlag != "" {
			target = common.HexToAddress(*targetFlag)
		} else {
			// default invalid: target CrossL2Inbox predeploy to trigger MessageTargetCrossL2Inbox
			target = common.HexToAddress("0x4200000000000000000000000000000000000022")
		}
		receipt, err := sendL2ToL2Message(srcRPC, dstChain, privKey, target, []byte{0x00})
		if err != nil {
			fmt.Printf("ERROR: initiating invalid message: %v\n", err)
			os.Exit(1)
		}
		_, sentPayload, id, err := buildRelayFromReceipt(srcRPC, receipt)
		if err != nil {
			fmt.Printf("ERROR: build relay payload: %v\n", err)
			os.Exit(1)
		}
		// Mutate Identifier to craft an invalid executing message (mismatched logIndex)
		if id[2] != nil {
			id[2] = new(big.Int).Add(id[2], big.NewInt(1))
		}
		// Attempt relay - may succeed on-chain but should be filtered out by supervisor
		recRelay, err := relayMessage(dstRPCRelay, privKey, id, sentPayload)
		if err != nil {
			// Transaction was likely filtered out by supervisor
			fmt.Println("Invalid Message: transaction filtered out")
		} else if recRelay == nil || recRelay.Status != types.ReceiptStatusSuccessful {
			fmt.Println("Invalid Message: transaction filtered out")
		} else {
			// Get block timestamp
			ctx, cancel := contextWithTimeout(5 * time.Second)
			cli, err := ethclient.DialContext(ctx, dstRPCRelay)
			var blockTimestamp uint64
			if err == nil {
				if header, err := cli.HeaderByNumber(ctx, recRelay.BlockNumber); err == nil {
					blockTimestamp = header.Time
				}
				cli.Close()
			}
			cancel()
			fmt.Printf("Invalid Message: ok (block %d, timestamp %d)\n", recRelay.BlockNumber.Uint64(), blockTimestamp)
		}
	case "tx+invalid-msg":
		if err := sendSimpleTxBoth([]string{*rpc901, *rpc902}, privKey, *timeout); err != nil {
			fmt.Printf("ERROR: send txs: %v\n", err)
			os.Exit(1)
		}
		srcRPC, dstChain := pickSrcDst(*fromChain, *rpc901, *rpc902)
		target := common.HexToAddress("0x4200000000000000000000000000000000000022")
		if _, err := sendL2ToL2Message(srcRPC, dstChain, privKey, target, []byte{0x00}); err != nil {
			fmt.Printf("ERROR: send L2->L2 invalid message: %v\n", err)
			os.Exit(1)
		}
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
		if strings.HasPrefix(line, "#") || line == "" {
			continue
		}
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
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasPrefix(name, "op-up.") {
			continue
		}
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

type crossSafeResp struct {
	Derived blockID `json:"derived"`
}

func waitSV2Advancing(sv2URL string, chainRPCs map[string]string, deadline time.Time, interval time.Duration) error {
	last := map[string]uint64{}
	client := &http.Client{Timeout: 3 * time.Second}
	for time.Now().Before(deadline) {
		allOK := true
		for id, rpcURL := range chainRPCs {
			req, _ := http.NewRequest(http.MethodGet, sv2URL+"/v1/cross_safe?chainId="+id, nil)
			resp, err := client.Do(req)
			if err != nil || resp == nil || resp.Body == nil {
				allOK = false
				break
			}
			var cs crossSafeResp
			_ = json.NewDecoder(resp.Body).Decode(&cs)
			resp.Body.Close()
			num := cs.Derived.Number
			if num == 0 || num <= last[id] {
				allOK = false
			} else {
				// Verify EL has the block at this number
				ctx, cancel := contextWithTimeout(5 * time.Second)
				cli, err := ethclient.DialContext(ctx, rpcURL)
				if err != nil {
					cancel()
					allOK = false
				} else {
					_, herr := cli.HeaderByNumber(ctx, new(big.Int).SetUint64(num))
					cli.Close()
					cancel()
					if herr != nil {
						allOK = false
					}
				}
			}
			last[id] = num
		}
		if allOK {
			return nil
		}
		time.Sleep(interval)
	}
	return fmt.Errorf("timeout waiting for SV2 to advance chains: %v", chainRPCs)
}

// buildRelayFromReceipt locates the SentMessage log in the initiating tx receipt and constructs
// the relay inputs (Identifier and sentMessage bytes) expected by L2ToL2CrossDomainMessenger.relayMessage.
func buildRelayFromReceipt(srcRPC string, receipt *types.Receipt) (*types.Log, []byte, [5]*big.Int, error) {
	// SentMessage event selector from contracts-bedrock
	sentSel := common.HexToHash("0x382409ac69001e11931a28435afef442cbfd20d9891907e8fa373ba7d351f320")
	var l *types.Log
	for _, lg := range receipt.Logs {
		if len(lg.Topics) >= 1 && lg.Topics[0] == sentSel {
			l = lg
			break
		}
	}
	if l == nil {
		return nil, nil, [5]*big.Int{}, fmt.Errorf("SentMessage log not found in receipt")
	}
	// Fetch the raw tx to reconstruct calldata sender for data portion
	ctx, cancel := contextWithTimeout(10 * time.Second)
	defer cancel()
	cli, err := ethclient.DialContext(ctx, srcRPC)
	if err != nil {
		return nil, nil, [5]*big.Int{}, err
	}
	defer cli.Close()
	tx, _, err := cli.TransactionByHash(ctx, receipt.TxHash)
	if err != nil {
		return nil, nil, [5]*big.Int{}, err
	}
	// Need sender to encode data portion (sender, message)
	signer := types.LatestSignerForChainID(tx.ChainId())
	from, err := types.Sender(signer, tx)
	if err != nil {
		return nil, nil, [5]*big.Int{}, err
	}
	// Topics layout for SentMessage: selector, destination(chainid), target(address), nonce(uint256)
	if len(l.Topics) < 4 {
		return nil, nil, [5]*big.Int{}, fmt.Errorf("unexpected topics len")
	}
	destination := new(big.Int).SetBytes(l.Topics[1].Bytes())
	_ = destination // not used for relay, but part of sent payload encoding preimage
	target := common.BytesToAddress(l.Topics[2].Bytes()[12:])
	nonce := new(big.Int).SetBytes(l.Topics[3].Bytes())
	// SentMessage data is abi.encode(sender, message)
	// We do not know the message payload; reconstruct from tx input after the function selector (best-effort).
	// For messenger.sendMessage(dst, target, message) the 3rd arg is the bytes payload.
	// Extract tx input and decode last parameter as bytes.
	input := tx.Data()
	// Try a naive ABI decode of (uint256,address,bytes). We don't pull ABI, so slice trailing bytes as message.
	// Use the standard solidity encoding: last param offset points to dynamic data; but to avoid full decode,
	// we rely on geth to return access list / or as a fallback call eth_getTransactionReceipt and fetch log.Data directly.
	// Since log.Data already contains abi.encode(sender,message), we can use it as sentMessage tail, prefix with topics preimage.
	// Construct sentMessage = abi.encode(topics...) || log.Data
	// topics preimage: abi.encode(SENT_SELECTOR, destination, target, nonce)
	chainID := tx.ChainId()
	var preimage []byte
	{
		// abi.encode(selector, destination, target, nonce)
		// encode each as 32-byte
		w := make([]byte, 0, 32*4)
		w = append(w, sentSel.Bytes()...)
		w = append(w, common.LeftPadBytes(destination.Bytes(), 32)...)
		w = append(w, common.LeftPadBytes(target.Bytes(), 32)...)
		w = append(w, common.LeftPadBytes(nonce.Bytes(), 32)...)
		preimage = w
	}
	// Debug context for relay encoding
	fmt.Printf("relay ctx: dest=%s target=%s nonce=%s from=%s l1=%d\n",
		destination.String(), target.Hex(), nonce.String(), from.Hex(), l.BlockNumber)
	// Construct final sentMessage: preimage || abi.encode(sender,message) as emitted in log.Data
	sentMessage := append(preimage, l.Data...)
	msgHash := crypto.Keccak256Hash(sentMessage)
	logDataHash := crypto.Keccak256Hash(l.Data)
	fmt.Printf("relay hashes: sentMessage=%s logData=%s preimageLen=%d dataLen=%d\n",
		msgHash.Hex(), logDataHash.Hex(), len(preimage), len(l.Data))
	// Build Identifier tuple (origin=messenger addr, blockNumber, logIndex, timestamp, chainId)
	// We don't have timestamp here without another call; fetch header.
	hdr, err := cli.HeaderByNumber(ctx, new(big.Int).SetUint64(l.BlockNumber))
	if err != nil {
		return nil, nil, [5]*big.Int{}, err
	}
	id := [5]*big.Int{new(big.Int), new(big.Int), new(big.Int), new(big.Int), new(big.Int)}
	// origin: messenger address (as big-int via address bytes)
	id[0].SetBytes(common.HexToAddress("0x4200000000000000000000000000000000000023").Bytes())
	id[1].SetUint64(l.BlockNumber)
	id[2].SetUint64(uint64(l.Index))
	id[3].SetUint64(hdr.Time)
	id[4].Set(chainID)
	_ = from // from not needed further; included in log.Data already
	_ = input
	return l, sentMessage, id, nil
}

// relayMessage calls MESSENGER.relayMessage on the destination chain using the provided tuple and payload.
func relayMessage(dstRPC string, privKey string, id [5]*big.Int, sentMessage []byte) (*types.Receipt, error) {
	rpc := dstRPC
	ctx, cancel := contextWithTimeout(90 * time.Second)
	defer cancel()
	pk, err := parseECDSA(privKey)
	if err != nil {
		return nil, err
	}
	cli, err := ethclient.DialContext(ctx, rpc)
	if err != nil {
		return nil, err
	}
	defer cli.Close()
	chainID, err := cli.ChainID(ctx)
	if err != nil {
		return nil, err
	}
	fmt.Printf("relayMessage: dialing dstRPC=%s chainID=%s\n", rpc, chainID.String())
	auth, err := bind.NewKeyedTransactorWithChainID(pk, chainID)
	if err != nil {
		return nil, err
	}
	tip, err := cli.SuggestGasTipCap(ctx)
	if err != nil {
		return nil, err
	}
	hdr, err := cli.HeaderByNumber(ctx, nil)
	if err != nil {
		return nil, err
	}
	base := hdr.BaseFee
	if base == nil {
		base = big.NewInt(0)
	}
	auth.GasTipCap = tip
	auth.GasFeeCap = new(big.Int).Add(new(big.Int).Mul(base, big.NewInt(2)), tip)
	auth.GasLimit = 1_500_000
	auth.Context = ctx
	// Build access list and call CrossL2Inbox.validateMessage to emit ExecutingMessage
	msgHash := crypto.Keccak256Hash(sentMessage)
	inboxAddr := common.HexToAddress("0x4200000000000000000000000000000000000022")
	ibID := inboxbinding.Identifier{Origin: common.HexToAddress("0x4200000000000000000000000000000000000023"), BlockNumber: id[1], LogIndex: id[2], Timestamp: id[3], ChainId: id[4]}
	// Encode access list entries using supervisor types (may require multiple storage keys)
	supID := suptypes.Identifier{Origin: ibID.Origin, BlockNumber: ibID.BlockNumber.Uint64(), LogIndex: uint32(ibID.LogIndex.Uint64()), Timestamp: ibID.Timestamp.Uint64(), ChainID: opeth.ChainIDFromUInt64(ibID.ChainId.Uint64())}
	access := supID.ChecksumArgs(msgHash).Access()
	al := types.AccessList{
		{Address: inboxAddr, StorageKeys: suptypes.EncodeAccessList([]suptypes.Access{access})},
	}
	// Submit via binding with access list
	auth.AccessList = al
	ib, err := inboxbinding.NewInbox(inboxAddr, cli)
	if err != nil {
		return nil, err
	}
	tx, err := ib.ValidateMessage(auth, ibID, msgHash)
	if err != nil {
		return nil, err
	}
	fmt.Printf("relay tx: %s\n", tx.Hash().Hex())
	r, err := bind.WaitMined(ctx, cli, tx)
	if err != nil {
		return nil, err
	}
	if r.Status != types.ReceiptStatusSuccessful {
		return r, fmt.Errorf("relay failed, status=%d", r.Status)
	}
	return r, nil
}

// relayMessageHash is like relayMessage, but uses a precomputed msgHash instead of the full sentMessage bytes.
func relayMessageHash(dstRPC string, privKey string, id [5]*big.Int, msgHash common.Hash) (*types.Receipt, error) {
	rpc := dstRPC
	ctx, cancel := contextWithTimeout(90 * time.Second)
	defer cancel()
	pk, err := parseECDSA(privKey)
	if err != nil {
		return nil, err
	}
	cli, err := ethclient.DialContext(ctx, rpc)
	if err != nil {
		return nil, err
	}
	defer cli.Close()
	chainID, err := cli.ChainID(ctx)
	if err != nil {
		return nil, err
	}
	fmt.Printf("relayMessageHash: dialing dstRPC=%s chainID=%s\n", rpc, chainID.String())
	auth, err := bind.NewKeyedTransactorWithChainID(pk, chainID)
	if err != nil {
		return nil, err
	}
	tip, err := cli.SuggestGasTipCap(ctx)
	if err != nil {
		return nil, err
	}
	hdr, err := cli.HeaderByNumber(ctx, nil)
	if err != nil {
		return nil, err
	}
	base := hdr.BaseFee
	if base == nil {
		base = big.NewInt(0)
	}
	auth.GasTipCap = tip
	auth.GasFeeCap = new(big.Int).Add(new(big.Int).Mul(base, big.NewInt(2)), tip)
	auth.GasLimit = 1_500_000
	auth.Context = ctx
	inboxAddr := common.HexToAddress("0x4200000000000000000000000000000000000022")
	// Build Identifier and access list from provided id
	originAddr := common.BytesToAddress(id[0].Bytes())
	ibID := inboxbinding.Identifier{Origin: originAddr, BlockNumber: id[1], LogIndex: id[2], Timestamp: id[3], ChainId: id[4]}
	supID := suptypes.Identifier{Origin: ibID.Origin, BlockNumber: ibID.BlockNumber.Uint64(), LogIndex: uint32(ibID.LogIndex.Uint64()), Timestamp: ibID.Timestamp.Uint64(), ChainID: opeth.ChainIDFromUInt64(ibID.ChainId.Uint64())}
	access := supID.ChecksumArgs(msgHash).Access()
	al := types.AccessList{{Address: inboxAddr, StorageKeys: suptypes.EncodeAccessList([]suptypes.Access{access})}}
	auth.AccessList = al
	ib, err := inboxbinding.NewInbox(inboxAddr, cli)
	if err != nil {
		return nil, err
	}
	tx, err := ib.ValidateMessage(auth, ibID, msgHash)
	if err != nil {
		return nil, err
	}
	fmt.Printf("relay tx: %s\n", tx.Hash().Hex())
	r, err := bind.WaitMined(ctx, cli, tx)
	if err != nil {
		return nil, err
	}
	if r.Status != types.ReceiptStatusSuccessful {
		return r, fmt.Errorf("relay failed, status=%d", r.Status)
	}
	return r, nil
}

// relayMessageHashBatch submits multiple identical CrossL2Inbox.validateMessage txs with ascending nonces as a JSON-RPC batch.
func relayMessageHashBatch(dstRPC string, privKey string, id [5]*big.Int, msgHash common.Hash, mult int) error {
	if mult <= 1 {
		_, err := relayMessageHash(dstRPC, privKey, id, msgHash)
		return err
	}
	ctx, cancel := contextWithTimeout(90 * time.Second)
	defer cancel()

	pk, err := parseECDSA(privKey)
	if err != nil {
		return err
	}
	cli, err := ethclient.DialContext(ctx, dstRPC)
	if err != nil {
		return err
	}
	defer cli.Close()
	chainID, err := cli.ChainID(ctx)
	if err != nil {
		return err
	}
	tip, err := cli.SuggestGasTipCap(ctx)
	if err != nil {
		return err
	}
	hdr, err := cli.HeaderByNumber(ctx, nil)
	if err != nil {
		return err
	}
	base := hdr.BaseFee
	if base == nil {
		base = big.NewInt(0)
	}
	gasTip := tip
	gasFee := new(big.Int).Add(new(big.Int).Mul(base, big.NewInt(2)), tip)

	// From address
	from := crypto.PubkeyToAddress(pk.PublicKey)

	// Build calldata for validateMessage(Identifier, bytes32)
	inboxAddr := common.HexToAddress("0x4200000000000000000000000000000000000022")
	// ABI encode call
	ibabi, err := abi.JSON(strings.NewReader(inboxbinding.InboxMetaData.ABI))
	if err != nil {
		return err
	}
	originAddr := common.BytesToAddress(id[0].Bytes())
	ibID := inboxbinding.Identifier{Origin: originAddr, BlockNumber: id[1], LogIndex: id[2], Timestamp: id[3], ChainId: id[4]}
	data, err := ibabi.Pack("validateMessage", ibID, msgHash)
	if err != nil {
		return err
	}

	// Access list
	supID := suptypes.Identifier{Origin: ibID.Origin, BlockNumber: ibID.BlockNumber.Uint64(), LogIndex: uint32(ibID.LogIndex.Uint64()), Timestamp: ibID.Timestamp.Uint64(), ChainID: opeth.ChainIDFromUInt64(ibID.ChainId.Uint64())}
	access := supID.ChecksumArgs(msgHash).Access()
	al := types.AccessList{{Address: inboxAddr, StorageKeys: suptypes.EncodeAccessList([]suptypes.Access{access})}}

	// Fetch base nonce
	baseNonce, err := cli.PendingNonceAt(ctx, from)
	if err != nil {
		return err
	}

	// Build and sign mult txs
	txs := make([]*types.Transaction, 0, mult)
	for i := 0; i < mult; i++ {
		tx := types.NewTx(&types.DynamicFeeTx{
			ChainID:    chainID,
			Nonce:      baseNonce + uint64(i),
			GasTipCap:  gasTip,
			GasFeeCap:  gasFee,
			Gas:        1_500_000,
			To:         &inboxAddr,
			Value:      big.NewInt(0),
			Data:       data,
			AccessList: al,
		})
		stx, err := types.SignTx(tx, types.NewLondonSigner(chainID), pk)
		if err != nil {
			return err
		}
		txs = append(txs, stx)
	}

	// Batch send
	rpcClient, err := rpc.DialContext(ctx, dstRPC)
	if err != nil {
		return err
	}
	defer rpcClient.Close()
	results := make([]string, len(txs))
	elems := make([]rpc.BatchElem, 0, len(txs))
	for i, tx := range txs {
		raw, err := tx.MarshalBinary()
		if err != nil {
			return err
		}
		elems = append(elems, rpc.BatchElem{Method: "eth_sendRawTransaction", Args: []interface{}{"0x" + common.Bytes2Hex(raw)}, Result: &results[i]})
	}
	if err := rpcClient.BatchCallContext(ctx, elems); err != nil {
		return err
	}
	for i, el := range elems {
		if el.Error != nil {
			fmt.Printf("batch[%d] error: %v\n", i, el.Error)
		} else if results[i] != "" {
			fmt.Printf("batch[%d] tx: %s\n", i, results[i])
		}
	}
	return nil
}

// waitSV2CrossPast waits until the CrossSafe head for a chain surpasses the given block number.
func waitSV2CrossPast(sv2URL string, chainID string, rpcURL string, minBlock uint64, deadline time.Time, interval time.Duration) error {
	client := &http.Client{Timeout: 3 * time.Second}
	var lastCS uint64
	for time.Now().Before(deadline) {
		req, _ := http.NewRequest(http.MethodGet, sv2URL+"/v1/cross_safe?chainId="+chainID, nil)
		resp, err := client.Do(req)
		if err == nil && resp != nil && resp.Body != nil {
			var cs crossSafeResp
			_ = json.NewDecoder(resp.Body).Decode(&cs)
			resp.Body.Close()
			cur := cs.Derived.Number
			if lastCS != cur {
				fmt.Printf("SV2 CS[%s] %d->%d (target>%d)\n", chainID, lastCS, cur, minBlock)
				lastCS = cur
			}
			if cur > minBlock {
				// verify EL has the block
				ctx, cancel := contextWithTimeout(5 * time.Second)
				cli, err := ethclient.DialContext(ctx, rpcURL)
				if err == nil {
					_, herr := cli.HeaderByNumber(ctx, new(big.Int).SetUint64(cur))
					cli.Close()
					cancel()
					if herr == nil {
						return nil
					}
				} else {
					cancel()
				}
			}
		}
		time.Sleep(interval)
	}
	return fmt.Errorf("timeout waiting CrossSafe[%s] > %d (last=%d)", chainID, minBlock, lastCS)
}

// waitSV2LocalSafeAtLeast waits until the LocalSafe head for a chain reaches at least the given block number.
func waitSV2LocalSafeAtLeast(sv2URL string, chainID string, rpcURL string, minBlock uint64, deadline time.Time, interval time.Duration) error {
	client := &http.Client{Timeout: 3 * time.Second}
	var last uint64
	for time.Now().Before(deadline) {
		req, _ := http.NewRequest(http.MethodGet, sv2URL+"/v1/cross_safe?chainId="+chainID, nil)
		resp, err := client.Do(req)
		if err == nil && resp != nil && resp.Body != nil {
			var cs crossSafeResp
			_ = json.NewDecoder(resp.Body).Decode(&cs)
			resp.Body.Close()
			cur := cs.Derived.Number
			if last != cur {
				fmt.Printf("SV2 cross_safe[%s] %d->%d (target>=%d)\n", chainID, last, cur, minBlock)
				last = cur
			}
			if cur >= minBlock {
				// verify EL has the block
				ctx, cancel := contextWithTimeout(5 * time.Second)
				cli, err := ethclient.DialContext(ctx, rpcURL)
				if err == nil {
					_, herr := cli.HeaderByNumber(ctx, new(big.Int).SetUint64(cur))
					cli.Close()
					cancel()
					if herr == nil {
						return nil
					}
				} else {
					cancel()
				}
			}
		}
		time.Sleep(interval)
	}
	return fmt.Errorf("timeout waiting cross_safe[%s] >= %d (last=%d)", chainID, minBlock, last)
}

// verifyCanonicalBlock checks that the block at a given number still has the expected hash on the source chain.
func verifyCanonicalBlock(srcRPC string, number *big.Int, expected common.Hash) error {
	ctx, cancel := contextWithTimeout(20 * time.Second)
	defer cancel()
	cli, err := ethclient.DialContext(ctx, srcRPC)
	if err != nil {
		return err
	}
	defer cli.Close()
	hdr, err := cli.HeaderByNumber(ctx, number)
	if err != nil {
		return err
	}
	if hdr.Hash() != expected {
		return fmt.Errorf("block hash mismatch at %s: got %s want %s", number.String(), hdr.Hash().Hex(), expected.Hex())
	}
	return nil
}

// calculateInboxChecksum mirrors CrossL2Inbox.calculateChecksum in Go for building the warm slot.
func calculateInboxChecksum(origin common.Address, blockNumber, logIndex, timestamp, chainId *big.Int, msgHash common.Hash) common.Hash {
	// logHash = keccak256(abi.encodePacked(origin, msgHash))
	logHash := crypto.Keccak256Hash(append(origin.Bytes(), msgHash.Bytes()...))
	// pack id: uint96(0), uint64(blockNumber), uint64(timestamp), uint32(logIndex)
	bn := new(big.Int).Set(blockNumber)
	ts := new(big.Int).Set(timestamp)
	li := new(big.Int).Set(logIndex)
	idPacked := make([]byte, 0, 12+8+8+4)
	idPacked = append(idPacked, make([]byte, 12)...)
	idPacked = append(idPacked, common.LeftPadBytes(new(big.Int).SetUint64(bn.Uint64()).Bytes(), 8)...)
	idPacked = append(idPacked, common.LeftPadBytes(new(big.Int).SetUint64(ts.Uint64()).Bytes(), 8)...)
	idPacked = append(idPacked, common.LeftPadBytes(new(big.Int).SetUint64(li.Uint64()).Bytes(), 4)...)
	idLogHash := crypto.Keccak256Hash(append(logHash.Bytes(), idPacked...))
	bare := crypto.Keccak256Hash(append(idLogHash.Bytes(), chainId.Bytes()...))
	// apply MSB mask zero and TYPE_3 (0x03) in MSB
	b := bare.Bytes()
	if len(b) != 32 {
		bb := make([]byte, 32)
		copy(bb[32-len(b):], b)
		b = bb
	}
	b[0] = 0x03
	var out common.Hash
	copy(out[:], b)
	return out
}

// sendSimpleTxBoth sends a 0 ETH self-transfer on each provided RPC URL, using the given private key
// If privKey is empty, a random key is used and the tx may fail due to insufficient funds.
func sendSimpleTxBoth(rpcs []string, privKey string, overallTimeout time.Duration) error {
	if len(rpcs) == 0 {
		return nil
	}
	for _, url := range rpcs {
		if strings.TrimSpace(url) == "" {
			continue
		}
		if err := sendSimpleTx(url, privKey, overallTimeout); err != nil {
			return fmt.Errorf("rpc %s: %w", url, err)
		}
	}
	return nil
}

// (removed old simple-tx batch helpers)
func sendBatchToRPC(rpcURL, privKey string, repeat int, overallTimeout time.Duration) error {
	ctx, cancel := contextWithTimeout(overallTimeout)
	defer cancel()

	cli, err := ethclient.DialContext(ctx, rpcURL)
	if err != nil {
		return err
	}
	defer cli.Close()

	// Parse key
	var pkObj *ecdsa.PrivateKey
	if privKey != "" {
		sk := strings.TrimPrefix(privKey, "0x")
		if pk, err := crypto.HexToECDSA(sk); err == nil {
			pkObj = pk
		} else {
			return fmt.Errorf("parse key: %w", err)
		}
	} else {
		tmp, err := crypto.GenerateKey()
		if err != nil {
			return err
		}
		pkObj = tmp
	}

	from := crypto.PubkeyToAddress(pkObj.PublicKey)
	to := from
	chainID, err := cli.ChainID(ctx)
	if err != nil {
		return err
	}
	baseNonce, err := cli.PendingNonceAt(ctx, from)
	if err != nil {
		return err
	}
	tipCap, err := cli.SuggestGasTipCap(ctx)
	if err != nil {
		return err
	}
	header, err := cli.HeaderByNumber(ctx, nil)
	if err != nil {
		return err
	}
	base := header.BaseFee
	if base == nil {
		base = big.NewInt(0)
	}
	feeCap := new(big.Int).Add(new(big.Int).Mul(base, big.NewInt(2)), tipCap)

	// Build and sign transactions with ascending nonce
	txs := make([]*types.Transaction, 0, repeat)
	for i := 0; i < repeat; i++ {
		tx := types.NewTx(&types.DynamicFeeTx{
			ChainID:   chainID,
			Nonce:     baseNonce + uint64(i),
			GasTipCap: tipCap,
			GasFeeCap: feeCap,
			Gas:       21000,
			To:        &to,
			Value:     big.NewInt(0),
			Data:      nil,
		})
		stx, err := types.SignTx(tx, types.NewLondonSigner(chainID), pkObj)
		if err != nil {
			return err
		}
		txs = append(txs, stx)
	}

	// Prepare batch of eth_sendRawTransaction calls
	type req struct {
		Method  string        `json:"method"`
		Params  []interface{} `json:"params"`
		ID      int           `json:"id"`
		JSONRPC string        `json:"jsonrpc"`
	}
	calls := make([]req, 0, len(txs))
	for i, tx := range txs {
		raw, err := tx.MarshalBinary()
		if err != nil {
			return err
		}
		calls = append(calls, req{Method: "eth_sendRawTransaction", Params: []interface{}{"0x" + common.Bytes2Hex(raw)}, ID: i + 1, JSONRPC: "2.0"})
	}

	// Use low-level RPC client for batch
	rpcClient, err := rpc.DialContext(ctx, rpcURL)
	if err != nil {
		return err
	}
	defer rpcClient.Close()

	// Prepare batch elems
	results := make([]string, len(calls))
	elems := make([]rpc.BatchElem, 0, len(calls))
	for i := range calls {
		elems = append(elems, rpc.BatchElem{
			Method: "eth_sendRawTransaction",
			Args:   calls[i].Params,
			Result: &results[i],
		})
	}
	// Execute batch
	if err := rpcClient.BatchCallContext(ctx, elems); err != nil {
		return err
	}

	// Print tx hashes for visibility
	for i, el := range elems {
		if el.Error != nil {
			fmt.Printf("batch[%d] error: %v\n", i, el.Error)
			continue
		}
		if results[i] != "" {
			fmt.Printf("batch[%d] tx: %s\n", i, results[i])
		}
	}
	return nil
}

func sendSimpleTx(rpcURL, privKey string, overallTimeout time.Duration) error {
	ctx, cancel := contextWithTimeout(overallTimeout)
	defer cancel()

	cli, err := ethclient.DialContext(ctx, rpcURL)
	if err != nil {
		return err
	}
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
		if err != nil {
			return err
		}
		pkObj = tmp
	}

	from := crypto.PubkeyToAddress(pkObj.PublicKey)
	to := from

	chainID, err := cli.ChainID(ctx)
	if err != nil {
		return err
	}
	nonce, err := cli.PendingNonceAt(ctx, from)
	if err != nil {
		return err
	}
	tipCap, err := cli.SuggestGasTipCap(ctx)
	if err != nil {
		return err
	}
	header, err := cli.HeaderByNumber(ctx, nil)
	if err != nil {
		return err
	}
	base := header.BaseFee
	if base == nil {
		base = big.NewInt(0)
	}
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
	if err != nil {
		return err
	}
	if err := cli.SendTransaction(ctx, stx); err != nil {
		return err
	}

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

// sendL2ToL2Message sends a message via L2ToL2CrossDomainMessenger on the source chain to the destination chain.
// target is an address on the destination chain. message is the calldata at the destination.
func sendL2ToL2Message(srcRPC string, dstChain string, privKey string, target common.Address, message []byte) (*types.Receipt, error) {
	ctx, cancel := contextWithTimeout(60 * time.Second)
	defer cancel()

	if strings.TrimSpace(privKey) == "" {
		return nil, fmt.Errorf("priv-key required for L2->L2 message")
	}
	pkObj, err := parseECDSA(privKey)
	if err != nil {
		return nil, fmt.Errorf("parse key: %w", err)
	}
	cli, err := ethclient.DialContext(ctx, srcRPC)
	if err != nil {
		return nil, err
	}
	defer cli.Close()

	chainID, err := cli.ChainID(ctx)
	if err != nil {
		return nil, err
	}
	auth, err := bind.NewKeyedTransactorWithChainID(pkObj, chainID)
	if err != nil {
		return nil, err
	}
	// Set EIP-1559 fees
	tipCap, err := cli.SuggestGasTipCap(ctx)
	if err != nil {
		return nil, err
	}
	header, err := cli.HeaderByNumber(ctx, nil)
	if err != nil {
		return nil, err
	}
	base := header.BaseFee
	if base == nil {
		base = big.NewInt(0)
	}
	auth.GasTipCap = tipCap
	auth.GasFeeCap = new(big.Int).Add(new(big.Int).Mul(base, big.NewInt(2)), tipCap)
	auth.GasLimit = 800000 // generous default
	auth.Context = ctx

	// Messenger predeploy address
	messengerAddr := common.HexToAddress("0x4200000000000000000000000000000000000023")
	messenger, err := bindings.NewL2ToL2CrossDomainMessenger(messengerAddr, cli)
	if err != nil {
		return nil, err
	}
	// Destination chain id as big.Int
	dstBig := new(big.Int)
	if _, ok := dstBig.SetString(strings.TrimSpace(dstChain), 10); !ok {
		return nil, fmt.Errorf("invalid dst chain id: %s", dstChain)
	}
	tx, err := messenger.SendMessage(auth, dstBig, target, message)
	if err != nil {
		return nil, err
	}
	fmt.Printf("l2->l2 sendMessage tx: %s (src=%s dst=%s target=%s)\n", tx.Hash().Hex(), srcRPC, dstChain, target.Hex())
	// Wait for inclusion
	receipt, err := bind.WaitMined(ctx, cli, tx)
	if err != nil {
		return nil, err
	}
	fmt.Printf("l2->l2 sendMessage mined: status=%d block=%s\n", receipt.Status, receipt.BlockNumber.String())
	return receipt, nil
}

func parseECDSA(privKey string) (*ecdsa.PrivateKey, error) {
	s := strings.TrimSpace(privKey)
	s = strings.TrimPrefix(strings.TrimPrefix(s, "0x"), "0X")
	if s == "" {
		return nil, fmt.Errorf("empty key")
	}
	return crypto.HexToECDSA(s)
}

// pickSrcDst selects the source RPC and destination chain id string based on the provided from flag.
func pickSrcDst(from string, rpc901, rpc902 string) (string, string) {
	switch strings.TrimSpace(from) {
	case "901":
		return rpc901, "902"
	case "902":
		return rpc902, "901"
	default:
		return rpc901, "902"
	}
}

// tiny helper to create a cancellable context with default if zero
func contextWithTimeout(d time.Duration) (context.Context, context.CancelFunc) {
	if d <= 0 {
		d = 30 * time.Second
	}
	return context.WithTimeout(context.Background(), d)
}

// showHeads prints EL latest/safe/finalized block numbers for both chains and the cross-safe derived heads from SV2.
func showHeads(sv2URL, rpc901, rpc902 string) error {
	if strings.TrimSpace(rpc901) == "" || strings.TrimSpace(rpc902) == "" {
		return fmt.Errorf("rpc-901 and rpc-902 required")
	}
	// discover SV2 if not provided
	sv2 := strings.TrimSpace(sv2URL)
	if sv2 == "" {
		if u, err := discoverSV2URL(); err == nil {
			sv2 = u
		} else {
			// fallback to common default from Kurtosis
			sv2 = "http://127.0.0.1:33321"
		}
	}
	// per chain helper
	query := func(label string, rpcURL string) error {
		ctx, cancel := contextWithTimeout(5 * time.Second)
		defer cancel()
		cli, err := ethclient.DialContext(ctx, rpcURL)
		if err != nil {
			return err
		}
		defer cli.Close()
		// latest (unsafe tip)
		latest, err := cli.HeaderByNumber(ctx, nil)
		if err != nil {
			return err
		}
		// safe and finalized via tags (rpc constants)
		safe, err := cli.HeaderByNumber(ctx, big.NewInt(int64(rpc.SafeBlockNumber)))
		if err != nil {
			return err
		}
		finalized, err := cli.HeaderByNumber(ctx, big.NewInt(int64(rpc.FinalizedBlockNumber)))
		if err != nil {
			return err
		}
		// cross-safe via SV2, resolve the actual chain ID from EL
		chainID, err := cli.ChainID(ctx)
		if err != nil {
			return err
		}
		var cross uint64
		if sv2 != "" {
			req, _ := http.NewRequest(http.MethodGet, sv2+"/v1/cross_safe?chainId="+chainID.String(), nil)
			resp, err := (&http.Client{Timeout: 3 * time.Second}).Do(req)
			if err == nil && resp != nil && resp.Body != nil {
				var x struct {
					Derived blockID `json:"derived"`
				}
				_ = json.NewDecoder(resp.Body).Decode(&x)
				resp.Body.Close()
				cross = x.Derived.Number
			}
		}
		fmt.Printf("chain %s(id=%s): latest=%d safe=%d finalized=%d cross_safe=%d\n", label, chainID.String(), latest.Number.Uint64(), safe.Number.Uint64(), finalized.Number.Uint64(), cross)
		return nil
	}
	if err := query("901", rpc901); err != nil {
		return fmt.Errorf("901: %w", err)
	}
	if err := query("902", rpc902); err != nil {
		return fmt.Errorf("902: %w", err)
	}
	return nil
}

package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"math/big"
	"strings"

	bss "github.com/ethereum-optimism/optimism/op-batcher/batcher"
	"github.com/ethereum-optimism/optimism/op-chain-ops/devkeys"
	"github.com/ethereum-optimism/optimism/op-devstack/devtest"
	"github.com/ethereum-optimism/optimism/op-devstack/presets"
	"github.com/ethereum-optimism/optimism/op-devstack/shim"
	"github.com/ethereum-optimism/optimism/op-devstack/stack"
	"github.com/ethereum-optimism/optimism/op-devstack/stack/match"
	"github.com/ethereum-optimism/optimism/op-devstack/sysgo"
	"github.com/ethereum-optimism/optimism/op-e2e/e2eutils/setuputils"
	"github.com/ethereum-optimism/optimism/op-service/client"
	"github.com/ethereum-optimism/optimism/op-service/endpoint"
	"github.com/ethereum-optimism/optimism/op-service/eth"
	oplog "github.com/ethereum-optimism/optimism/op-service/log"
	"github.com/ethereum-optimism/optimism/op-service/log/logfilter"
	"github.com/ethereum-optimism/optimism/op-service/testreq"
	supv2 "github.com/ethereum-optimism/optimism/op-supervisor-v2/supervisor"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"go.opentelemetry.io/otel/trace"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v", err)
		os.Exit(1)
	}
}

func run() error {
	opDir, ok := os.LookupEnv("OP_DIR")
	if !ok {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			return fmt.Errorf("get user home dir: %w", err)
		}
		opDir = filepath.Join(homeDir, ".op")
	}
	if err := os.MkdirAll(opDir, 0o755); err != nil {
		return fmt.Errorf("create the op dir: %w", err)
	}
	deployerCacheDir := filepath.Join(opDir, "deployer", "cache")
	if err := os.MkdirAll(deployerCacheDir, 0o755); err != nil {
		return fmt.Errorf("create the deployer cache dir: %w", err)
	}

	ids := sysgo.NewDefaultMinimalSystemIDs(sysgo.DefaultL1ID, sysgo.DefaultL2AID)
	var opt stack.Option[*sysgo.Orchestrator]
	if os.Getenv("OP_EXTERNAL_L1") == "1" {
		// Derive L1/L2 IDs from env for faucet wiring
		l1CID := sysgo.DefaultL1ID
		if v := os.Getenv("OP_L1_CHAIN_ID"); v != "" {
			if n, ok := new(big.Int).SetString(v, 10); ok {
				l1CID = eth.ChainIDFromUInt64(n.Uint64())
			}
		}
		l2CID := sysgo.DefaultL2AID
		if v := os.Getenv("OP_L2_CHAIN_ID"); v != "" {
			if n, ok := new(big.Int).SetString(v, 10); ok {
				l2CID = eth.ChainIDFromUInt64(n.Uint64())
			}
		}
		l1ELForFaucet := stack.NewL1ELNodeID("l1", l1CID)
		l2ELForFaucet := stack.NewL2ELNodeID("sequencer", l2CID)

		opt = stack.Combine(
			sysgo.WithMnemonicKeys(devkeys.TestMnemonic),
			sysgo.WithExternalPresetFromEnv(),
			sysgo.WithL2ELNode(ids.L2EL, nil),
			sysgo.WithSupervisorV2OnFirstChain(),
			// Use L1_PK for batcher transactions on Sepolia
			sysgo.WithBatcherOption(func(id stack.L2BatcherID, cfg *bss.CLIConfig) {
				pk := os.Getenv("L1_PK")
				if pk != "" {
					if strings.HasPrefix(pk, "0x") || strings.HasPrefix(pk, "0X") {
						pk = pk[2:]
					}
					if sk, err := crypto.HexToECDSA(pk); err == nil {
						cfg.TxMgrConfig = setuputils.NewTxMgrConfig(endpoint.URL(os.Getenv("OP_L1_RPC")), sk)
					}
				}
			}),
			// Start the batcher to post data to L1 so safe can progress
			sysgo.WithBatcher(ids.L2Batcher, l1ELForFaucet, stack.NewL2CLNodeID("embedded", l2CID), ids.L2EL),
			// Enable faucets for convenience (L2 funding only; L1 faucet requires funded key)
			sysgo.WithFaucets([]stack.L1ELNodeID{l1ELForFaucet}, []stack.L2ELNodeID{l2ELForFaucet}),
		)
	} else {
		opt = stack.Combine(
			sysgo.WithMnemonicKeys(devkeys.TestMnemonic),
			sysgo.WithDeployer(),
			sysgo.WithDeployerOptions(
				sysgo.WithLocalContractSources(),
				sysgo.WithCommons(ids.L1.ChainID()),
				sysgo.WithPrefundedL2(ids.L1.ChainID(), ids.L2.ChainID()),
			),
			sysgo.WithDeployerPipelineOption(sysgo.WithDeployerCacheDir(deployerCacheDir)),
			sysgo.WithL1Nodes(ids.L1EL, ids.L1CL),
			sysgo.WithL2ELNode(ids.L2EL, nil),
			sysgo.WithL2CLNode(ids.L2CL, true, false, ids.L1CL, ids.L1EL, ids.L2EL),
			sysgo.WithBatcher(ids.L2Batcher, ids.L1EL, ids.L2CL, ids.L2EL),
			sysgo.WithProposer(ids.L2Proposer, ids.L1EL, &ids.L2CL, nil),
			sysgo.WithFaucets([]stack.L1ELNodeID{ids.L1EL}, []stack.L2ELNodeID{ids.L2EL}),
		)
	}
	presets.DoMain(testingM{}, stack.MakeCommon(opt), presets.WithLogFilter(logfilter.DefaultShow(logfilter.Level(log.LevelDebug).Show())))

	return nil
}

type testingM struct{}

var _ presets.TestingM = testingM{}

func (t testingM) Run() int {
	if err := runSysgo(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v", err)
		return 1
	}
	return 0
}

func runSysgo() error {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	// Auto-stop duration is configurable via OP_UP_STOP_AFTER (default 15s)
	stopAfter := 15 * time.Second
	if v := os.Getenv("OP_UP_STOP_AFTER"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			stopAfter = d
		} else if n, err := atoiStrict(v); err == nil && n > 0 {
			stopAfter = time.Duration(n) * time.Second
		}
	}
	stop := time.AfterFunc(stopAfter, cancel)
	defer func() {
		stop.Stop()
		cancel()
	}()

	// Print available account.
	hd, err := devkeys.NewMnemonicDevKeys(devkeys.TestMnemonic)
	if err != nil {
		return fmt.Errorf("new mnemonic dev keys: %w", err)
	}
	const funderIndex = 10_000 // see sysgo/deployer.go.
	funderUserKey := devkeys.UserKey(funderIndex)
	funderAddress, err := hd.Address(funderUserKey)
	if err != nil {
		return fmt.Errorf("address: %w", err)
	}
	funderPrivKey, err := hd.Secret(funderUserKey)
	if err != nil {
		return fmt.Errorf("secret: %w", err)
	}

	fmt.Printf("Test Account Address: %s\n", funderAddress)
	fmt.Printf("Test Account Private Key: %s\n", "0x"+common.Bytes2Hex(crypto.FromECDSA(funderPrivKey)))
	fmt.Printf("EL Node URL: %s\n", "http://localhost:8545")

	orch := presets.Orchestrator()
	t := &testingT{
		ctx:      ctx,
		cleanups: make([]func(), 0),
	}
	defer t.doCleanup()
	sys := shim.NewSystem(t)
	orch.Hydrate(sys)
	l2Networks := sys.L2Networks()
	if len(l2Networks) != 1 {
		return fmt.Errorf("need one l2 network, got: %d", len(l2Networks))
	}
	l2Net := l2Networks[0]
	elNode := l2Net.L2ELNode(match.FirstL2EL)

	// Log on new blocks.
	go func() {
		const blockPollInterval = 500 * time.Millisecond
		var lastBlock uint64
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(blockPollInterval):
				unsafe, err := elNode.EthClient().BlockRefByLabel(ctx, eth.Unsafe)
				if err != nil {
					continue
				}
				if unsafe.Number != lastBlock {
					fmt.Printf("New L2 block: number %d, hash %s\n", unsafe.Number, unsafe.Hash)
					lastBlock = unsafe.Number
				}
			}
		}
	}()

	// Proxy L2 EL requests.
	go func() {
		if err := proxyEL(elNode.L2EthClient().RPC()); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v", err)
		}
	}()

	// Start supervisor-v2 polling against sysgo L2
	{
		// Use the RollupAPI from the L2 CL shim directly and the EL RPC from the L2 EL shim
		roll := l2Net.L2CLNode(match.FirstL2CL).RollupAPI()
		elUserRPC := elNode.L2EthClient().RPC()

		logCfg := oplog.DefaultCLIConfig()
		lgr := oplog.NewLogger(os.Stdout, logCfg)
		rcfg, err := roll.RollupConfig(t.Ctx())
		if err != nil {
			return err
		}
		s := supv2.NewSupervisor(lgr)
		// Expose SV2 HTTP for health and /opnode/ proxy so external tools can query sync status
		s.EnableOpNodeProxy(true)
		go func() {
			addr := "127.0.0.1:9750"
			if err := http.ListenAndServe(addr, s.HTTPHandler()); err != nil {
				fmt.Fprintf(os.Stderr, "sv2 http listen error: %v\n", err)
			}
		}()
		if err := s.StartPollingWithRollupClient(roll, elUserRPC, rcfg, time.Second, 40); err != nil {
			return err
		}
	}

	<-ctx.Done()

	return nil
}

// proxyEL is a hacky way to intercept EL json rpc requests for logging to get around log filtering
// bugs.
func proxyEL(client client.RPC) error {
	// Set up the HTTP handler for all incoming requests.
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// Ensure the request method is POST, as JSON RPC typically uses POST.
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Read the entire request body.
		requestBody, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Failed to read request body", http.StatusInternalServerError)
			return
		}
		defer r.Body.Close() // Close the request body after reading

		// Parse the incoming JSON RPC request. We use a map to dynamically
		// extract the method, parameters, and ID.
		var req map[string]any
		if err := json.Unmarshal(requestBody, &req); err != nil {
			http.Error(w, "Invalid JSON RPC request format", http.StatusBadRequest)
			return
		}

		// Extract the RPC method name.
		method, ok := req["method"].(string)
		if !ok {
			http.Error(w, "Missing or invalid 'method' field in JSON RPC request", http.StatusBadRequest)
			return
		}

		// Extract RPC parameters. JSON RPC parameters can be an array, an object, or null/missing.
		var callParams []any
		if p, ok := req["params"]; ok && p != nil {
			if arr, isArray := p.([]any); isArray {
				// If parameters are an array, spread them directly.
				callParams = arr
			} else if obj, isObject := p.(map[string]any); isObject {
				// If parameters are a JSON object, pass the entire object as a single argument.
				callParams = []any{obj}
			} else {
				http.Error(w, "Invalid 'params' field in JSON RPC request (must be array, object, or null)", http.StatusBadRequest)
				return
			}
		}
		// If 'params' is missing or null, `callParams` remains empty, which is correct for methods without parameters.

		// Extract the request ID. This is crucial for matching responses to requests.
		id := req["id"] // ID can be string, number, or null. We don't need to check `ok` for this.

		// Prepare a variable to hold the RPC response result.
		// `json.RawMessage` is used to capture the raw JSON value from the backend
		// without needing to know its specific Go type beforehand.
		var rpcResult json.RawMessage

		// Create a context with a timeout for the RPC call to the backend.
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second) // 30-second timeout
		defer cancel()                                                           // Ensure the context is cancelled to release resources

		fmt.Println(method)

		// Use the rpc.Client to make the actual call to the backend Ethereum node.
		// The `callParams...` syntax unpacks the slice into variadic arguments.
		err = client.CallContext(ctx, &rpcResult, method, callParams...)
		if err != nil {
			message := fmt.Sprintf("RPC call to backend failed for method '%s': %v", method, err)
			// If the RPC call to the backend fails, construct a JSON RPC error response.
			rpcErr := map[string]any{
				"jsonrpc": "2.0",
				"id":      id,
				"error": map[string]any{
					"code":    -32000, // Standard JSON RPC server error code for internal errors
					"message": message,
				},
			}
			fmt.Printf("RPC error: %s\n", message)
			jsonResponse, _ := json.Marshal(rpcErr) // Marshaling error is unlikely here, so we ignore it.
			w.Header().Set("Content-Type", "application/json")
			// For JSON-RPC, errors are typically returned with an HTTP 200 OK status,
			// with the error details within the JSON payload.
			w.WriteHeader(http.StatusOK)
			if _, err := w.Write(jsonResponse); err != nil {
				return
			}
			return
		}

		// If the RPC call was successful, construct the JSON RPC success response.
		responseMap := map[string]any{
			"jsonrpc": "2.0",
			"id":      id,
			"result":  rpcResult, // The raw JSON result from the backend node
		}

		jsonResponse, err := json.Marshal(responseMap)
		if err != nil {
			http.Error(w, "Failed to marshal RPC success response", http.StatusInternalServerError)
			return
		}

		// Set the Content-Type header and write the successful JSON RPC response.
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write(jsonResponse); err != nil {
			return
		}
	})

	// Start the HTTP server.
	if err := http.ListenAndServe("localhost:8545", nil); err != nil {
		return fmt.Errorf("listen and server: %w", err)
	}
	return nil
}

type testingT struct {
	mu       sync.Mutex
	ctx      context.Context
	cleanups []func()
}

var _ devtest.T = (*testingT)(nil)
var _ testreq.TestingT = (*testingT)(nil)

func (t *testingT) doCleanup() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for i := len(t.cleanups) - 1; i >= 0; i-- {
		t.cleanups[i]()
	}
}

// Cleanup implements devtest.T.
func (t *testingT) Cleanup(fn func()) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.cleanups = append(t.cleanups, fn)
}

// Ctx implements devtest.T.
func (t *testingT) Ctx() context.Context {
	return t.ctx
}

// Deadline implements devtest.T.
func (t *testingT) Deadline() (deadline time.Time, ok bool) {
	return time.Time{}, false
}

// Error implements devtest.T.
func (t *testingT) Error(args ...any) {
}

// Errorf implements devtest.T.
func (t *testingT) Errorf(format string, args ...any) {
}

// Fail implements devtest.T.
func (t *testingT) Fail() {
}

// FailNow implements devtest.T.
func (t *testingT) FailNow() {
}

// Gate implements devtest.T.
func (t *testingT) Gate() *testreq.Assertions {
	return testreq.New(t)
}

// Helper implements devtest.T.
func (t *testingT) Helper() {
}

// Log implements devtest.T.
func (t *testingT) Log(args ...any) {
}

// Logf implements devtest.T.
func (t *testingT) Logf(format string, args ...any) {
}

func (t *testingT) Logger() log.Logger {
	return log.NewLogger(slog.NewTextHandler(io.Discard, nil))
}

func (t *testingT) Name() string {
	return "dev"
}

func (t *testingT) Parallel() {
}

func (t *testingT) Require() *testreq.Assertions {
	return testreq.New(t)
}

func (t *testingT) Run(name string, fn func(devtest.T)) {
	panic("unimplemented")
}

func (t *testingT) Skip(args ...any) {
	panic("unimplemented")
}

func (t *testingT) SkipNow() {
	panic("unimplemented")
}

// Skipf implements devtest.T.
func (t *testingT) Skipf(format string, args ...any) {
	panic("unimplemented")
}

// Skipped implements devtest.T.
func (t *testingT) Skipped() bool {
	return false
}

// TempDir implements devtest.T.
func (t *testingT) TempDir() string {
	panic("unimplemented")
}

// Tracer implements devtest.T.
func (t *testingT) Tracer() trace.Tracer {
	panic("unimplemented")
}

// WithCtx implements devtest.T.
func (t *testingT) WithCtx(ctx context.Context) devtest.T {
	return t
}

// _TestOnly implements devtest.T.
func (t *testingT) TestOnly() {
}

// atoiStrict attempts to parse an integer from a string without allowing
// trailing characters. Returns an error if parsing fails.
func atoiStrict(s string) (int, error) {
	var n int
	// Fast path: try standard Atoi
	if _, err := fmt.Sscanf(s, "%d", &n); err != nil {
		return 0, err
	}
	return n, nil
}

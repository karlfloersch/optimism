## SV2 multi-chain (901, 902) on Sepolia — debugging status

### Goal
- Run two L2 chains (901, 902) against external L1 (Sepolia) with a single Supervisor v2 (SV2) embedding op-nodes, and verify safe head progression on both chains.

### Setup highlights
- One SV2 instance supervising both chains; batchers target SV2 op-node proxies:
  - 901: `http://127.0.0.1:<sv2-port>/opnode/901/`
  - 902: `http://127.0.0.1:<sv2-port>/opnode/902/`
- Two distinct L2 ELs (op-geth) started per run on unique ports (examples from runs):
  - 901 EL userRPC/authRPC: `127.0.0.1:51715/51716` (earlier) or `127.0.0.1:54012/54013` (later run)
  - 902 EL userRPC/authRPC: `127.0.0.1:51720/51721` (earlier) or `127.0.0.1:54017/54018` (later run)
- External beacon (CL REST) validated to respond 200 OK:
  - `GET <beacon>/eth/v1/beacon/headers/head` returns JSON

### Key code/config changes used
- `op-up/justfile`: `deploy2` sleep to avoid nonce contention; `run2` to launch SV2 with multiple chains, sourcing `external-l1.env` and artifact paths.
- `op-up/main.go`: multi-chain external-L1 mode to:
  - Register external L1 RPC/beacon
  - Load per-chain artifacts (rollup + l2_genesis)
  - Start one SV2 across all chains
  - Start batchers after SV2 is ready; batchers target SV2 op-node proxy; per-chain batcher key selection
  - Optional cadence envs (`OP_BATCHER_MAX_CHANNEL_DURATION`, `OP_BATCHER_POLL_INTERVAL`)
- `op-devstack/sysgo/supervisor_v2.go`: tolerate nil beacon; use HTTP beacon addr when no in-process beacon is present.

### Evidence that wiring is correct
- Batcher startup shows per-chain rollup RPC targeting correct proxy:
  - `chain=901 ... rollup=[http://127.0.0.1:51722/opnode/901/]`
  - `chain=902 ... rollup=[http://127.0.0.1:51722/opnode/902/]`
- Two ELs per run, distinct ports (examples):
  - `HTTP server started endpoint=127.0.0.1:51715/51716` and `127.0.0.1:51720/51721`
  - or `127.0.0.1:54012/54013` and `127.0.0.1:54017/54018`
- Proxy traces confirm 901 ws-proxy forwards to 901 EL ports, and 902 ws-proxy forwards to 902 EL ports (distinct upstreams).
- Rollup configs (via artifacts and SV2) match per chain:
  - 901 l2 genesis: `0x456633...` chainId 901
  - 902 l2 genesis: `0x4adff7...` chainId 902

### Observed behavior (most recent 15-min run)
- 901: unsafe advanced to ~1206; safe/local_safe = 643 (healthy and stable).
- 902: unsafe advanced to ~935; safe/local_safe remain 0 (no promotion).
- Batchers for both chains continuously publish and confirm on L1 (numerous `Transaction confirmed` entries for both chainIDs).

### Recurrent error pattern (especially on 902)
- Frequent attribute mismatches causing reorgs:
  - `L2 reorg: existing unsafe block does not match derived attributes from L1 ... expected: <PrevRandao A> got: <PrevRandao B>`
  - Observed repeatedly at many heights (e.g., 529, 535, 547, 558, 594, 607, 613, 637, etc.).
- Occasional receipt fetch fallbacks (provider does not expose `debug_getRawReceipts`):
  - Falls back to `eth_getTransactionReceipt (batched)`
  - `Engine temporary error ... failed to fetch receipts ... debug_getRawReceipts does not exist/is not available`

### Interpretation
- This is not an EL mix-up or genesis mismatch:
  - Distinct ELs per chain with distinct ports and correct proxy wiring.
  - Rollup configs (L2 genesis and chain IDs) match artifacts and per chain.
- 902 repeatedly derives L2 blocks whose header attributes (notably PrevRandao) do not match the CL-derived attributes for the same L1 origin. The op-node reorgs those blocks before promoting local_safe, keeping safe at 0. 901 also sees mismatches occasionally but still stabilizes and promotes to local_safe/safe.
- Upgrading the beacon improved availability, but 902 continues to reorg often enough to block safe promotion in these runs.

### Next steps
1. Confirm EL→chain mapping and genesis directly via EL RPC:
   - 901 EL `eth_chainId` on userRPC and `eth_getBlockByNumber("0x0")` root
   - 902 EL `eth_chainId` and block 0
2. Capture detailed 902 reorg windows (PrevRandao/timestamp) adjacent to each `L2 reorg` line to verify the exact mismatch pattern vs. CL data.
3. Run a longer session (10–15+ min) after beacon upgrade; watch for reduction in attribute mismatches and eventual local_safe promotion on 902.
4. If needed, temporarily stagger batcher cadence per chain or increase channel duration to reduce derivation pressure while confirming stability.

### Quick TL;DR
- 901: good (safe=643).
- 902: unsafe advances; safe stuck at 0 due to repeated attribute (PrevRandao) mismatches and reorgs. Wiring/configs look correct; issue lies in CL-attribute consistency for 902.



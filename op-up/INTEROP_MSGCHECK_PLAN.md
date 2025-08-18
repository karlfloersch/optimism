## Interop msgcheck plan (Sepolia-backed)

### Goals
- Stand up two fresh L2s against Sepolia (only Interop2; NOT using the Interop hardfork; Holocene enabled).
- Let the network run ~15 minutes in the background to stabilize.
- Ship a Go tool that submits valid and invalid executing messages and verifies outcomes end-to-end (invalid is reorged out automatically).

### Assumptions
- Env file: `op-up/external-l1.env` contains Sepolia/RPC endpoints, keys (incl. faucet), and any required URLs.
- Chain selection: any two new L2s from the deployment (A and B); not critical which IDs.
- Keys: Use faucet private key (or any convenient funded key from the env) to sign txs; no extra funding flow required.
- Timeouts: Single end-to-end timeout of ~10 minutes per run with ~250 ms polling cadence is acceptable.
- Denylist: No manual POSTs; invalid executing message must naturally trigger rollback and reorg.

### High-level steps
1) Deploy two new L2s against Sepolia
   - Follow `op-up` README for Sepolia external-L1 deployment.
   - Use existing configs (`op-up/external-l1.env` plus the repo’s documented steps) to bring up an interop network with two L2s.
   - Ensure Holocene is active; only Interop2 is enabled (as in current defaults).

2) Warm up and keep running ~15 minutes
   - After boot, leave the network running to build a few dozen L2 blocks on both chains.
   - We will only use GET endpoints from SV2 for readiness (e.g., `/v1/sync_status`).

3) Implement msgcheck tool
   - Location: `op-up/cmd/msgcheck`
   - Flags:
     - `--env-file` (default: `op-up/external-l1.env`)
     - `--mode` = `valid|invalid|both` (default: `both`)
     - `--timeout` (default: `10m`)
     - `--poll-interval` (default: `250ms`)
     - `--log-file` (optional)
   - Behavior:
     - Parse endpoints/keys from the env file (L1 HTTP, L2A/L2B EL RPCs, SV2 base URL if present).
     - Readiness: poll SV2 `/v1/sync_status` per chain until unsafe/safe advance.
     - Valid flow:
       - Construct and submit a valid executing message (mirrors sysgo test pattern).
       - Wait for inclusion/effect on the destination chain; confirm by block hash/receipts.
     - Invalid flow:
       - Construct and submit an invalid executing message (same pattern as in tests, i.e., references an invalid initiating message).
       - Observe initial inclusion (if any), then wait for reorg (block replacement at the relevant height and/or tx disappears by hash).
       - Success = invalid executing message is reorged out.
     - Logging: structured info (heights, hashes, tx IDs, timing). Exit non-zero on timeout/failure.

4) Run against Sepolia only
   - Command example:
     - `go run ./op-up/cmd/msgcheck --env-file op-up/external-l1.env --mode=both --timeout=10m --poll-interval=250ms --log-file logs/msgcheck.sepolia.log`
   - Capture outputs and logs for audit.

5) (Optional) Containerize later
   - Once stable locally, add a container wrapper and run the same workflow in CI or a long-lived environment.

### Milestones (checklist)
- [ ] Deploy two new L2s against Sepolia via `op-up` with `external-l1.env` (Interop2 only; Holocene enabled)
- [ ] Leave network running ~15 minutes; verify heads advance on both L2s
- [ ] Implement `op-up/cmd/msgcheck` (flags, env parsing, RPC wiring, readiness)
- [ ] Implement valid flow (submit + verify inclusion)
- [ ] Implement invalid flow (submit + verify reorg)
- [ ] Run msgcheck against Sepolia; collect logs and hashes
- [ ] Review results; iterate if timing/edge cases require tuning

### Notes / open items
- SV2 base URL env var: If not present in the env file, msgcheck will only rely on L2 RPCs; readiness can be derived from RPC heads as a fallback.
- Faucet usage: Either sign directly with faucet key or re-use any funded dev key in the env; whichever is simpler in code.
- Executing-message construction: copy/port the minimal logic from sysgo tests to ensure parity with test semantics.


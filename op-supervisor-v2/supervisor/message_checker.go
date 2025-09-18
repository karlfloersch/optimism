package supervisor

import (
	"context"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	gn "github.com/ethereum/go-ethereum/node"
	"github.com/ethereum/go-ethereum/rpc"

	opclient "github.com/ethereum-optimism/optimism/op-service/client"
	"github.com/ethereum-optimism/optimism/op-service/sources"
	supervisorTypes "github.com/ethereum-optimism/optimism/op-supervisor/supervisor/types"
)

type checkAccessListCache map[string]bool

func (c *checkAccessListCache) Add(k supervisorTypes.Access, v bool) {
	enc := supervisorTypes.EncodeAccessList([]supervisorTypes.Access{k})[0]
	key := fmt.Sprintf("%s-%s", enc.Hex(), k.Checksum.String())
	(*c)[key] = v
}

func (c checkAccessListCache) Get(k supervisorTypes.Access) (bool, bool) {
	enc := supervisorTypes.EncodeAccessList([]supervisorTypes.Access{k})[0]
	key := fmt.Sprintf("%s-%s", enc.Hex(), k.Checksum.String())
	v, ok := c[key]
	return v, ok
}

// CheckAccessList validates an access list of messages, parsing each entry and checking if the referenced logs exist.
// This method provides the same functionality as the CheckAccessList function in the original supervisor.
//
// Parameters:
//   - ctx: Context for the operation
//   - inboxEntries: Array of encoded access list entries
//   - minSafety: Minimum safety level required for validation
//   - execDescr: Executing descriptor with chain ID, timestamp, and timeout
//
// Returns:
//   - Error if validation fails or any message is not found
func (s *Supervisor) CheckAccessList(ctx context.Context, inboxEntries []common.Hash, minSafety supervisorTypes.SafetyLevel, execDescr supervisorTypes.ExecutingDescriptor) error {
	s.log.Info("Starting access list validation", "function", "CheckAccessList", "entries", len(inboxEntries), "min_safety", minSafety)

	// Check if access list checking is enabled
	s.mu.Lock()
	enabled := s.checkAccessListEnabled
	s.mu.Unlock()

	if !enabled {
		s.log.Info("Access list validation disabled, returning success", "function", "CheckAccessList")
		return nil
	}

	// Parse and validate each access entry
	entries := inboxEntries
	for len(entries) > 0 {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("stopped access-list check early: %w", err)
		}

		// Parse the next access entry
		remaining, acc, err := supervisorTypes.ParseAccess(entries)
		if err != nil {
			return fmt.Errorf("failed to parse access entry: %w", err)
		}
		entries = remaining

		s.log.Info("Validating access entry", "function", "CheckAccessList", "chain_id", acc.ChainID, "block_number", acc.BlockNumber, "log_index", acc.LogIndex, "timestamp", acc.Timestamp)

		// Validate the access entry using our existing CheckMessage functionality
		err = s.checkAccessEntry(ctx, acc)
		if err != nil {
			return fmt.Errorf("access validation failed for chain %s, block %d, log %d: %w", acc.ChainID, acc.BlockNumber, acc.LogIndex, err)
		}
	}

	s.log.Info("All access list entries validated successfully", "function", "CheckAccessList")
	return nil
}

// checkAccessEntry validates a single access entry by checking if the referenced log exists and matches the checksum
func (s *Supervisor) checkAccessEntry(ctx context.Context, acc supervisorTypes.Access) error {
	chainID, _ := acc.ChainID.Uint64()

	v, ok := s.checkAccessListCache.Get(acc)
	if ok {
		// cached response: valid
		if v {
			s.log.Info("CheckAccessList: cached response: valid", "function", "checkAccessEntry", "chain_id", chainID, "block_number", acc.BlockNumber, "log_index", acc.LogIndex, "timestamp", acc.Timestamp, "checksum", acc.Checksum)
			return nil
		}
		// cached response: invalid
		s.log.Info("CheckAccessList: cached response: invalid", "function", "checkAccessEntry", "chain_id", chainID, "block_number", acc.BlockNumber, "log_index", acc.LogIndex, "timestamp", acc.Timestamp, "checksum", acc.Checksum)
		return fmt.Errorf("cached response: invalid")
	}
	s.log.Info("CheckAccessList: cached response: not cached", "function", "checkAccessEntry", "chain_id", chainID, "block_number", acc.BlockNumber, "log_index", acc.LogIndex, "timestamp", acc.Timestamp, "checksum", acc.Checksum)

	// Get the chain container for the specified chain ID
	s.mu.Lock()
	container := s.chains[chainID]
	s.mu.Unlock()

	if container == nil {
		s.log.Error("Unknown chain ID", "function", "checkAccessEntry", "chain_id", chainID)
		return fmt.Errorf("unknown chain ID: %d", chainID)
	}

	// Get the execution engine RPC endpoint and JWT secret from the virtual node config
	container.stateMu.Lock()
	l2AuthRPC := ""
	var jwtSecret [32]byte
	if container.virtualCfg != nil {
		l2AuthRPC = container.virtualCfg.L2AuthRPC
		jwtSecret = container.virtualCfg.JwtSecret
	}
	container.stateMu.Unlock()

	if l2AuthRPC == "" {
		s.log.Error("No execution engine RPC configured for chain", "function", "checkAccessEntry", "chain_id", chainID)
		return fmt.Errorf("no execution engine RPC configured for chain %d", chainID)
	}

	// Create execution engine client with JWT authentication
	auth := rpc.WithHTTPAuth(gn.NewJWTAuth(jwtSecret))
	client, err := opclient.NewRPC(ctx, s.log, l2AuthRPC, opclient.WithGethRPCOptions(auth))
	if err != nil {
		s.log.Error("Failed to connect to execution engine", "function", "checkAccessEntry", "chain_id", chainID, "error", err)
		return fmt.Errorf("failed to connect to execution engine: %w", err)
	}
	defer client.Close()

	// Create L2 client
	execClient, err := sources.NewL2Client(client, s.log, nil, sources.L2ClientDefaultConfig(container.virtualCfg.Rcfg, false))
	if err != nil {
		s.log.Error("Failed to create L2 client", "function", "checkAccessEntry", "chain_id", chainID, "error", err)
		s.checkAccessListCache.Add(acc, false)
		return fmt.Errorf("failed to create L2 client: %w", err)
	}

	// Get block by number
	blockRef, err := execClient.L2BlockRefByNumber(ctx, acc.BlockNumber)
	if err != nil {
		s.log.Error("Failed to get block by number", "function", "checkAccessEntry", "chain_id", chainID, "block_number", acc.BlockNumber, "error", err)
		s.checkAccessListCache.Add(acc, false)
		return fmt.Errorf("failed to get block %d: %w", acc.BlockNumber, err)
	}

	// Verify timestamp matches
	if blockRef.Time != acc.Timestamp {
		s.log.Error("Timestamp mismatch", "function", "checkAccessEntry", "chain_id", chainID, "expected", acc.Timestamp, "got", blockRef.Time)
		s.checkAccessListCache.Add(acc, false)
		return fmt.Errorf("timestamp mismatch: expected %d, got %d", acc.Timestamp, blockRef.Time)
	}

	// Get block receipts
	_, receipts, err := execClient.FetchReceipts(ctx, blockRef.Hash)
	if err != nil {
		s.log.Error("Failed to fetch receipts", "function", "checkAccessEntry", "chain_id", chainID, "block_hash", blockRef.Hash, "error", err)
		s.checkAccessListCache.Add(acc, false)
		return fmt.Errorf("failed to fetch receipts for block %s: %w", blockRef.Hash, err)
	}

	// Find the log at the specified index using double nested loop
	var targetLog *types.Log
	currentLogIdx := uint32(0)

	for _, receipt := range receipts {
		for _, log := range receipt.Logs {
			if currentLogIdx == acc.LogIndex {
				targetLog = log
				break
			}
			currentLogIdx++
		}
		if targetLog != nil {
			break
		}
	}

	if targetLog == nil {
		s.log.Error("Log index not found", "function", "checkAccessEntry", "chain_id", chainID, "log_index", acc.LogIndex, "block_number", acc.BlockNumber)
		s.checkAccessListCache.Add(acc, false)
		return fmt.Errorf("log index %d not found in block %d", acc.LogIndex, acc.BlockNumber)
	}

	// Calculate the checksum for the found log using original supervisor's ChecksumArgs
	// Use the original supervisor's checksum calculation
	payloadHash := crypto.Keccak256Hash(supervisorTypes.LogToMessagePayload(targetLog))
	logHash := supervisorTypes.PayloadHashToLogHash(payloadHash, targetLog.Address)

	checksumArgs := supervisorTypes.ChecksumArgs{
		BlockNumber: acc.BlockNumber,
		LogIndex:    acc.LogIndex,
		Timestamp:   acc.Timestamp,
		ChainID:     acc.ChainID,
		LogHash:     logHash,
	}
	actualChecksum := checksumArgs.Checksum()

	// Log both checksums for debugging
	s.log.Debug("Checksum comparison", "function", "checkAccessEntry",
		"expected", common.Hash(acc.Checksum).Hex(),
		"calculated", common.Hash(actualChecksum).Hex(),
		"chain_id", chainID, "block_number", acc.BlockNumber, "log_index", acc.LogIndex)

	// Verify checksum matches
	if actualChecksum != acc.Checksum {
		s.log.Error("Checksum mismatch", "function", "checkAccessEntry", "chain_id", chainID, "expected", acc.Checksum, "got", actualChecksum)
		s.checkAccessListCache.Add(acc, false)
		return fmt.Errorf("checksum mismatch: expected %s, got %s", common.Hash(acc.Checksum).Hex(), common.Hash(actualChecksum).Hex())
	}

	s.log.Debug("Checksum matches", "function", "checkAccessEntry", "chain_id", chainID, "checksum", actualChecksum)
	s.checkAccessListCache.Add(acc, true)
	return nil
}

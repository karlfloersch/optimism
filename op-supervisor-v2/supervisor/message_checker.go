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
	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum-optimism/optimism/op-service/sources"
	supervisorTypes "github.com/ethereum-optimism/optimism/op-supervisor/supervisor/types"
)

// CheckMessage validates if a message exists at the specified location and matches the provided checksum.
// This method provides the same functionality as the Contains function in the original supervisor.
//
// Parameters:
//   - ctx: Context for the operation
//   - chainID: The chain ID to check
//   - timestamp: Expected block timestamp
//   - blockNum: Block number containing the log
//   - logIdx: Index of the log within the block
//   - checksum: Expected message checksum (hex string)
//
// Returns:
//   - BlockSeal information if the message is found and valid
//   - Error if validation fails or message is not found
func (s *Supervisor) CheckMessage(ctx context.Context, chainID uint64, timestamp, blockNum uint64, logIdx uint32, checksumHex string) (supervisorTypes.BlockSeal, error) {
	s.log.Info("checkMessage: starting", "chainID", chainID, "timestamp", timestamp, "blockNum", blockNum, "logIdx", logIdx, "checksumHex", checksumHex)
	// Check if message checking is enabled
	s.mu.Lock()
	enabled := s.checkMessageEnabled
	s.mu.Unlock()

	if !enabled {
		// Return stub response when checking is disabled
		return supervisorTypes.BlockSeal{
			Hash:      common.Hash{}, // empty hash
			Number:    blockNum,
			Timestamp: timestamp,
		}, nil
	}

	// Parse checksum from hex string
	checksumBytes := common.FromHex(checksumHex)
	if len(checksumBytes) != 32 {
		s.log.Error("checkMessage: invalid checksum length", "expected", 32, "got", len(checksumBytes))
		return supervisorTypes.BlockSeal{}, fmt.Errorf("invalid checksum length: expected 32 bytes, got %d", len(checksumBytes))
	}

	// Get chain container
	s.mu.Lock()
	container := s.chains[chainID]
	s.mu.Unlock()

	if container == nil {
		s.log.Error("checkMessage: unknown chain ID", "chainID", chainID)
		return supervisorTypes.BlockSeal{}, fmt.Errorf("unknown chain ID: %d", chainID)
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
		s.log.Error("checkMessage: no execution engine RPC configured for chain", "chainID", chainID)
		return supervisorTypes.BlockSeal{}, fmt.Errorf("no execution engine RPC configured for chain %d", chainID)
	}

	// Create execution engine client with JWT authentication
	auth := rpc.WithHTTPAuth(gn.NewJWTAuth(jwtSecret))
	client, err := opclient.NewRPC(ctx, s.log, l2AuthRPC, opclient.WithGethRPCOptions(auth))
	if err != nil {
		s.log.Error("checkMessage: failed to connect to execution engine", "chainID", chainID, "error", err)
		return supervisorTypes.BlockSeal{}, fmt.Errorf("failed to connect to execution engine: %w", err)
	}
	defer client.Close()

	// Create execution client wrapper
	execClient, err := sources.NewL2Client(client, s.log, nil, sources.L2ClientDefaultConfig(container.virtualCfg.Rcfg, false))
	if err != nil {
		s.log.Error("checkMessage: failed to create L2 client", "chainID", chainID, "error", err)
		return supervisorTypes.BlockSeal{}, fmt.Errorf("failed to create L2 client: %w", err)
	}

	// Get the block by number to validate timestamp
	blockRef, err := execClient.L2BlockRefByNumber(ctx, blockNum)
	if err != nil {
		s.log.Error("checkMessage: failed to get block by number", "chainID", chainID, "blockNum", blockNum, "error", err)
		return supervisorTypes.BlockSeal{}, fmt.Errorf("failed to get block %d: %w", blockNum, err)
	}

	// Verify timestamp matches
	if blockRef.Time != timestamp {
		s.log.Error("checkMessage: timestamp mismatch", "chainID", chainID, "expected", timestamp, "got", blockRef.Time)
		return supervisorTypes.BlockSeal{}, fmt.Errorf("timestamp mismatch: expected %d, got %d", timestamp, blockRef.Time)
	}

	// Get block receipts
	_, receipts, err := execClient.FetchReceipts(ctx, blockRef.Hash)
	if err != nil {
		s.log.Error("checkMessage: failed to fetch receipts", "chainID", chainID, "blockHash", blockRef.Hash, "error", err)
		return supervisorTypes.BlockSeal{}, fmt.Errorf("failed to fetch receipts for block %s: %w", blockRef.Hash, err)
	}

	// Find the log at the specified index using double nested loop
	var targetLog *types.Log
	currentLogIdx := uint32(0)

	for _, receipt := range receipts {
		for _, log := range receipt.Logs {
			if currentLogIdx == logIdx {
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
		s.log.Error("checkMessage: log index not found", "chainID", chainID, "logIdx", logIdx, "blockNum", blockNum)
		return supervisorTypes.BlockSeal{}, fmt.Errorf("log index %d not found in block %d", logIdx, blockNum)
	}

	// Calculate the checksum for the found log using original supervisor's ChecksumArgs
	expectedChecksum := supervisorTypes.MessageChecksum(common.BytesToHash(checksumBytes))

	// Use the original supervisor's checksum calculation
	payloadHash := crypto.Keccak256Hash(supervisorTypes.LogToMessagePayload(targetLog))
	logHash := supervisorTypes.PayloadHashToLogHash(payloadHash, targetLog.Address)

	checksumArgs := supervisorTypes.ChecksumArgs{
		BlockNumber: blockNum,
		LogIndex:    logIdx,
		Timestamp:   timestamp,
		ChainID:     eth.ChainIDFromUInt64(chainID),
		LogHash:     logHash,
	}
	actualChecksum := checksumArgs.Checksum()

	// Log both checksums for debugging
	fmt.Printf("checkMessage: Expected checksum: %s\n", common.Hash(expectedChecksum).Hex())
	fmt.Printf("checkMessage: Calculated checksum: %s\n", common.Hash(actualChecksum).Hex())
	fmt.Printf("checkMessage: Log data: %x\n", targetLog.Data)
	fmt.Printf("checkMessage: Log address: %s\n", targetLog.Address.Hex())
	fmt.Printf("checkMessage: Block: %d, Timestamp: %d, LogIdx: %d, ChainID: %d\n", blockNum, timestamp, logIdx, chainID)

	// Verify checksum matches
	if actualChecksum != expectedChecksum {
		s.log.Error("checkMessage: checksum mismatch", "chainID", chainID, "expected", expectedChecksum, "got", actualChecksum)
		return supervisorTypes.BlockSeal{}, fmt.Errorf("checksum mismatch: expected %s, got %s", common.Hash(expectedChecksum).Hex(), common.Hash(actualChecksum).Hex())
	}

	s.log.Info("checkMessage: checksum matches", "chainID", chainID, "expected", expectedChecksum, "got", actualChecksum)
	// Return block seal information
	return supervisorTypes.BlockSeal{
		Hash:      blockRef.Hash,
		Number:    blockRef.Number,
		Timestamp: blockRef.Time,
	}, nil
}

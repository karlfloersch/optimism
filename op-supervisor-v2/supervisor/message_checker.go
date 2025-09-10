package supervisor

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"

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
		return supervisorTypes.BlockSeal{}, fmt.Errorf("invalid checksum length: expected 32 bytes, got %d", len(checksumBytes))
	}

	// Get chain container
	s.mu.Lock()
	container := s.chains[chainID]
	s.mu.Unlock()

	if container == nil {
		return supervisorTypes.BlockSeal{}, fmt.Errorf("unknown chain ID: %d", chainID)
	}

	// Get the execution engine RPC endpoint from the virtual node config
	container.stateMu.Lock()
	l2AuthRPC := ""
	if container.virtualCfg != nil {
		l2AuthRPC = container.virtualCfg.L2AuthRPC
	}
	container.stateMu.Unlock()

	if l2AuthRPC == "" {
		return supervisorTypes.BlockSeal{}, fmt.Errorf("no execution engine RPC configured for chain %d", chainID)
	}

	// Create execution engine client
	client, err := opclient.NewRPC(ctx, s.log, l2AuthRPC)
	if err != nil {
		return supervisorTypes.BlockSeal{}, fmt.Errorf("failed to connect to execution engine: %w", err)
	}
	defer client.Close()

	// Create execution client wrapper
	execClient, err := sources.NewL2Client(client, s.log, nil, sources.L2ClientDefaultConfig(container.virtualCfg.Rcfg, false))
	if err != nil {
		return supervisorTypes.BlockSeal{}, fmt.Errorf("failed to create L2 client: %w", err)
	}

	// Get the block by number to validate timestamp
	blockRef, err := execClient.L2BlockRefByNumber(ctx, blockNum)
	if err != nil {
		return supervisorTypes.BlockSeal{}, fmt.Errorf("failed to get block %d: %w", blockNum, err)
	}

	// Verify timestamp matches
	if blockRef.Time != timestamp {
		return supervisorTypes.BlockSeal{}, fmt.Errorf("timestamp mismatch: expected %d, got %d", timestamp, blockRef.Time)
	}

	// Get block receipts
	_, receipts, err := execClient.FetchReceipts(ctx, blockRef.Hash)
	if err != nil {
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
		return supervisorTypes.BlockSeal{}, fmt.Errorf("log index %d not found in block %d", logIdx, blockNum)
	}

	// Calculate the checksum for the found log
	expectedChecksum := supervisorTypes.MessageChecksum(common.BytesToHash(checksumBytes))
	actualChecksum := s.calculateMessageChecksum(chainID, blockNum, timestamp, logIdx, targetLog)

	// Verify checksum matches
	if actualChecksum != expectedChecksum {
		return supervisorTypes.BlockSeal{}, fmt.Errorf("checksum mismatch: expected %s, got %s", expectedChecksum, actualChecksum)
	}

	// Return block seal information
	return supervisorTypes.BlockSeal{
		Hash:      blockRef.Hash,
		Number:    blockRef.Number,
		Timestamp: blockRef.Time,
	}, nil
}

// calculateMessageChecksum calculates the message checksum using the same algorithm as the original supervisor.
// Based on the ChecksumArgs.Checksum() method in op-supervisor/supervisor/types/types.go
func (s *Supervisor) calculateMessageChecksum(chainID uint64, blockNum, timestamp uint64, logIdx uint32, log *types.Log) supervisorTypes.MessageChecksum {
	// Create the log hash from the log data
	// This follows the PayloadHashToLogHash pattern from the original supervisor
	logHash := crypto.Keccak256Hash(log.Data)

	// Pack the identifier data exactly as in the original supervisor:
	// 12 zero bytes + blockNumber (8 bytes) + timestamp (8 bytes) + logIndex (4 bytes) = 32 bytes total
	idPacked := make([]byte, 32)
	// First 12 bytes remain zero (padding)
	// Use binary.BigEndian to match the original implementation
	binary.BigEndian.PutUint64(idPacked[12:20], blockNum)
	binary.BigEndian.PutUint64(idPacked[20:28], timestamp)
	binary.BigEndian.PutUint32(idPacked[28:32], logIdx)

	// Hash the log hash with the identifier
	idLogHash := crypto.Keccak256Hash(logHash[:], idPacked)

	// Create chain ID bytes (32 bytes)
	chainIDHash := eth.ChainIDFromUInt64(chainID).Bytes32()

	// Final hash: idLogHash + chainID
	out := crypto.Keccak256Hash(idLogHash[:], chainIDHash[:])

	// Set version/type byte
	out[0] = 0x03

	return supervisorTypes.MessageChecksum(out)
}

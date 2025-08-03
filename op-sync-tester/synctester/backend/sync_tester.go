package backend

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sync"

	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum-optimism/optimism/op-sync-tester/metrics"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/holiman/uint256"

	"github.com/ethereum-optimism/optimism/op-sync-tester/synctester/backend/config"
	sttypes "github.com/ethereum-optimism/optimism/op-sync-tester/synctester/backend/types"
	"github.com/ethereum-optimism/optimism/op-sync-tester/synctester/frontend"
)

var (
	ErrNoSession  = errors.New("no session")
	ErrNoReceipts = errors.New("no receipts")
)

type SyncTester struct {
	mu sync.RWMutex

	log log.Logger
	m   metrics.Metricer

	id       sttypes.SyncTesterID
	chainID  eth.ChainID
	elClient *ethclient.Client

	sessions map[string]*Session

	// Payload building tracking
	payloadBuilds sync.Map // map[eth.PayloadID]*PayloadBuildJob
}

// PayloadBuildJob tracks a payload building request
type PayloadBuildJob struct {
	ID              eth.PayloadID
	Attributes      *eth.PayloadAttributes
	ForkchoiceState *eth.ForkchoiceState
	CreatedAt       uint64
	Built           bool
}

var _ frontend.SyncBackend = (*SyncTester)(nil)
var _ frontend.EngineBackend = (*SyncTester)(nil)
var _ frontend.EthBackend = (*SyncTester)(nil)

func SyncTesterFromConfig(logger log.Logger, m metrics.Metricer, stID sttypes.SyncTesterID, stCfg *config.SyncTesterEntry) (*SyncTester, error) {
	logger = logger.New("syncTester", stID, "chain", stCfg.ChainID)
	elClient, err := ethclient.Dial(stCfg.ELRPC.Value.RPC())
	if err != nil {
		return nil, fmt.Errorf("failed to dial EL client: %w", err)
	}
	return &SyncTester{
		log:      logger,
		m:        m,
		id:       stID,
		chainID:  stCfg.ChainID,
		elClient: elClient,
		sessions: make(map[string]*Session),
	}, nil
}

func (s *SyncTester) fetchSession(ctx context.Context) (*Session, error) {
	session, ok := SessionFromContext(ctx)
	if !ok || session == nil {
		return nil, fmt.Errorf("no session found in context")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.sessions[session.SessionID]; ok {
		s.log.Debug("🔄 Reusing persistent session", "sessionID", session.SessionID, 
			"latest", existing.AvailableLatestHead, "safe", existing.AvailableSafeHead, "finalized", existing.AvailableFinalizedHead)
		return existing, nil  // Return the persistent session!
	} else {
		s.sessions[session.SessionID] = session
		s.log.Info("💾 Created new persistent session", "sessionID", session.SessionID)
		return session, nil  // Return the new session
	}
}

func (s *SyncTester) GetSession(ctx context.Context) error {
	// example session logic
	_, err := s.fetchSession(ctx)
	if err != nil {
		return ErrNoSession
	}
	return nil
}

func (s *SyncTester) DeleteSession(ctx context.Context) error {
	return nil
}

func (s *SyncTester) ListSessions(ctx context.Context) ([]string, error) {
	return []string{}, nil
}

func (s *SyncTester) GetBlockReceipts(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash) ([]*types.Receipt, error) {
	// Direct proxy - receipts don't need session offset logic
	return s.elClient.BlockReceipts(ctx, blockNrOrHash)
}

func (s *SyncTester) GetBlockByHash(ctx context.Context, hash common.Hash, fullTx bool) (interface{}, error) {
	// Direct proxy - hash lookups don't need session offset logic
	// Use raw RPC call to preserve all JSON fields
	var result interface{}
	err := s.elClient.Client().CallContext(ctx, &result, "eth_getBlockByHash", hash, fullTx)
	if err != nil {
		s.log.Error("Failed RPC call for block by hash", "hash", hash, "error", err)
		return nil, fmt.Errorf("failed RPC call: %w", err)
	}

	s.log.Info("Got block by hash", "hash", hash)
	return result, nil
}

func (s *SyncTester) GetBlockByNumber(ctx context.Context, number rpc.BlockNumber, fullTx bool) (interface{}, error) {
	// Apply session offset logic for block number requests
	resolvedNumber, err := s.resolveBlockNumber(ctx, number)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve block number: %w", err)
	}

	// Convert resolved number to hex string for RPC call
	var blockNumberParam string
	if resolvedNumber == nil {
		blockNumberParam = "latest"
	} else {
		blockNumberParam = fmt.Sprintf("0x%x", resolvedNumber)
	}

	s.log.Info("Making RPC call for block", "requested", number, "resolved", blockNumberParam)

	// Use raw RPC call to preserve all JSON fields
	var result interface{}
	err = s.elClient.Client().CallContext(ctx, &result, "eth_getBlockByNumber", blockNumberParam, fullTx)
	if err != nil {
		s.log.Error("Failed RPC call for block", "error", err)
		return nil, fmt.Errorf("failed RPC call: %w", err)
	}

	if result == nil {
		return nil, fmt.Errorf("block not found")
	}

	s.log.Info("Got complete block JSON", "requested", number, "resolved", blockNumberParam)
	return result, nil
}

// resolveBlockNumber resolves block numbers based on progress-driven session state
func (s *SyncTester) resolveBlockNumber(ctx context.Context, requestedNumber rpc.BlockNumber) (*big.Int, error) {
	// Check if this request has a session context and get persistent session
	_, hasSession := SessionFromContext(ctx)
	if !hasSession {
		// No session - convert rpc.BlockNumber to *big.Int and pass through
		if requestedNumber == rpc.LatestBlockNumber {
			return nil, nil // nil means latest for ethclient
		} else if requestedNumber == rpc.EarliestBlockNumber {
			return big.NewInt(0), nil
		} else if requestedNumber == rpc.PendingBlockNumber {
			return nil, nil // treat pending as latest for now
		} else {
			return big.NewInt(int64(requestedNumber)), nil
		}
	}

	// Get persistent session (this handles initialization automatically)
	session, err := s.fetchSession(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch persistent session: %w", err)
	}

	// Initialize session if needed
	err = s.initializeProgressSession(ctx, session)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize progress session: %w", err)
	}

	// Get current available heads from persistent session
	latest, safe, finalized := session.GetAvailableHeads()

	// Handle special cases for latest/safe/finalized based on available progress
	if requestedNumber == rpc.LatestBlockNumber {
		return big.NewInt(int64(latest)), nil
	} else if requestedNumber == rpc.EarliestBlockNumber {
		return big.NewInt(0), nil
	} else if requestedNumber == rpc.PendingBlockNumber {
		// Treat pending as latest
		return big.NewInt(int64(latest)), nil
	} else if requestedNumber == rpc.FinalizedBlockNumber {
		return big.NewInt(int64(finalized)), nil
	} else if requestedNumber == rpc.SafeBlockNumber {
		return big.NewInt(int64(safe)), nil
	} else {
		// For specific block numbers, pass through as-is
		return big.NewInt(int64(requestedNumber)), nil
	}
}

// initializeProgressSession initializes session with starting block positions for progress-driven sync
func (s *SyncTester) initializeProgressSession(ctx context.Context, session *Session) error {
	session.mu.Lock()
	defer session.mu.Unlock()
	
	// Check if already initialized - this should prevent re-initialization
	if session.Initialized {
		s.log.Debug("Session already initialized, skipping", 
			"sessionID", session.SessionID,
			"latest", session.AvailableLatestHead,
			"safe", session.AvailableSafeHead, 
			"finalized", session.AvailableFinalizedHead)
		return nil
	}

	// Get current chain tip ONCE to set absolute starting positions
	currentBlockNumber, err := s.elClient.BlockNumber(ctx)
	if err != nil {
		return fmt.Errorf("failed to get current block number: %w", err)
	}

	// Calculate ABSOLUTE starting positions (these won't change)
	if currentBlockNumber >= session.InitialLatestOffset {
		session.AvailableLatestHead = currentBlockNumber - session.InitialLatestOffset
	} else {
		session.AvailableLatestHead = 1 // Start from genesis if offset too large
	}
	
	if currentBlockNumber >= session.InitialSafeOffset {
		session.AvailableSafeHead = currentBlockNumber - session.InitialSafeOffset
	} else {
		session.AvailableSafeHead = 1
	}
	
	if currentBlockNumber >= session.InitialFinalizedOffset {
		session.AvailableFinalizedHead = currentBlockNumber - session.InitialFinalizedOffset
	} else {
		session.AvailableFinalizedHead = 1
	}
	
	// Mark as initialized to prevent recalculation
	session.Initialized = true
	
	s.log.Info("🎯 LOCKED-IN progress-driven session", 
		"sessionID", session.SessionID,
		"chainTipWhenInitialized", currentBlockNumber,
		"ABSOLUTE_startingLatest", session.AvailableLatestHead, 
		"ABSOLUTE_startingSafe", session.AvailableSafeHead, 
		"ABSOLUTE_startingFinalized", session.AvailableFinalizedHead)
	
	return nil
}

// advanceSessionProgress updates available heads when op-node successfully processes a block via ForkchoiceUpdated
func (s *SyncTester) advanceSessionProgress(ctx context.Context, processedBlock uint64) {
	_, hasSession := SessionFromContext(ctx)
	if !hasSession {
		return // No session context, nothing to update
	}

	// Get persistent session
	session, err := s.fetchSession(ctx)
	if err != nil {
		s.log.Warn("Failed to fetch persistent session for progress advancement", "error", err)
		return
	}

	// Get current Sepolia chain tip to avoid advancing past real chain
	currentSepolia, err := s.elClient.BlockNumber(ctx)
	if err != nil {
		s.log.Warn("Failed to get current Sepolia tip, cannot advance progress", "error", err)
		return
	}

	// Don't advance past real chain tip
	if processedBlock >= currentSepolia {
		s.log.Debug("Not advancing - processed block at/past Sepolia tip", 
			"processed", processedBlock, "sepolia_tip", currentSepolia)
		return
	}

	// Get before state for logging
	beforeLatest, beforeSafe, beforeFinalized := session.GetAvailableHeads()
	
	// Advance progress based on op-node's successful processing
	session.AdvanceProgress(processedBlock)
	
	// Get after state for logging
	afterLatest, afterSafe, afterFinalized := session.GetAvailableHeads()

	s.log.Info("🚀 op-node progress advanced session",
		"sessionID", session.SessionID,
		"processedBlock", processedBlock,
		"latest", fmt.Sprintf("%d→%d", beforeLatest, afterLatest),
		"safe", fmt.Sprintf("%d→%d", beforeSafe, afterSafe),
		"finalized", fmt.Sprintf("%d→%d", beforeFinalized, afterFinalized))
}

func (s *SyncTester) ChainId(ctx context.Context) (*hexutil.Big, error) {
	return (*hexutil.Big)(s.chainID.ToBig()), nil
}

// Engine API methods - these need to make direct RPC calls since ethclient doesn't expose them
func (s *SyncTester) GetPayloadV1(ctx context.Context, payloadID eth.PayloadID) (*eth.ExecutionPayload, error) {
	s.log.Info("GetPayloadV1 requested", "payloadID", payloadID)
	return s.buildMockPayload(ctx, payloadID)
}

func (s *SyncTester) GetPayloadV2(ctx context.Context, payloadID eth.PayloadID) (*eth.ExecutionPayloadEnvelope, error) {
	s.log.Warn("GetPayloadV2 requested but only v4 supported", "payloadID", payloadID)
	return nil, fmt.Errorf("payload %s not found: only v4 supported for recent blocks", payloadID.String())
}

func (s *SyncTester) GetPayloadV3(ctx context.Context, payloadID eth.PayloadID) (*eth.ExecutionPayloadEnvelope, error) {
	s.log.Warn("GetPayloadV3 requested but only v4 supported", "payloadID", payloadID)
	return nil, fmt.Errorf("payload %s not found: only v4 supported for recent blocks", payloadID.String())
}

func (s *SyncTester) GetPayloadV4(ctx context.Context, payloadID eth.PayloadID) (*eth.ExecutionPayloadEnvelope, error) {
	s.log.Info("GetPayloadV4 requested", "payloadID", payloadID)
	return s.buildMockPayloadV4(ctx, payloadID)
}

func (s *SyncTester) ForkchoiceUpdatedV1(ctx context.Context, state *eth.ForkchoiceState, attr *eth.PayloadAttributes) (*eth.ForkchoiceUpdatedResult, error) {
	s.log.Warn("ForkchoiceUpdatedV1 requested but only v3+ supported for recent blocks", "headBlockHash", state.HeadBlockHash)
	return &eth.ForkchoiceUpdatedResult{
		PayloadStatus: eth.PayloadStatusV1{
			Status:          "INVALID",
			ValidationError: stringPtr("only v3+ supported for recent blocks"),
		},
	}, nil
}

func (s *SyncTester) ForkchoiceUpdatedV2(ctx context.Context, state *eth.ForkchoiceState, attr *eth.PayloadAttributes) (*eth.ForkchoiceUpdatedResult, error) {
	s.log.Warn("ForkchoiceUpdatedV2 requested but preferring v3+ for recent blocks", "headBlockHash", state.HeadBlockHash)
	return s.mockForkchoiceUpdated(ctx, state, attr)
}

func (s *SyncTester) ForkchoiceUpdatedV3(ctx context.Context, state *eth.ForkchoiceState, attr *eth.PayloadAttributes) (*eth.ForkchoiceUpdatedResult, error) {
	s.log.Info("ForkchoiceUpdatedV3 requested", "headBlockHash", state.HeadBlockHash)
	return s.mockForkchoiceUpdated(ctx, state, attr)
}

// mockForkchoiceUpdated validates forkchoice state against Sepolia backend and returns appropriate response
func (s *SyncTester) mockForkchoiceUpdated(ctx context.Context, state *eth.ForkchoiceState, attr *eth.PayloadAttributes) (*eth.ForkchoiceUpdatedResult, error) {
	s.log.Info("Validating ForkchoiceUpdated", "headBlockHash", state.HeadBlockHash, "safeBlockHash", state.SafeBlockHash, "finalizedBlockHash", state.FinalizedBlockHash)

	// Validate all block hashes exist in the backend
	headBlock, err := s.validateBlockExists(ctx, state.HeadBlockHash, "head")
	if err != nil {
		return s.forkchoiceInvalidResult(state.HeadBlockHash, err.Error()), nil
	}

	safeBlock, err := s.validateBlockExists(ctx, state.SafeBlockHash, "safe")
	if err != nil {
		return s.forkchoiceInvalidResult(state.SafeBlockHash, err.Error()), nil
	}

	finalizedBlock, err := s.validateBlockExists(ctx, state.FinalizedBlockHash, "finalized")
	if err != nil {
		return s.forkchoiceInvalidResult(state.FinalizedBlockHash, err.Error()), nil
	}

	// Validate chain relationships: head >= safe >= finalized
	if err := s.validateForkchoiceChain(headBlock, safeBlock, finalizedBlock); err != nil {
		return s.forkchoiceInvalidResult(state.HeadBlockHash, err.Error()), nil
	}

	s.log.Info("ForkchoiceUpdated validation successful", "headNumber", headBlock.NumberU64(), "safeNumber", safeBlock.NumberU64(), "finalizedNumber", finalizedBlock.NumberU64())

	// Advance session progress - op-node successfully processed headBlock
	s.advanceSessionProgress(ctx, headBlock.NumberU64())

	// Return VALID status with the head block hash as latest valid
	result := &eth.ForkchoiceUpdatedResult{
		PayloadStatus: eth.PayloadStatusV1{
			Status:          "VALID",
			LatestValidHash: &state.HeadBlockHash,
		},
	}

	// Generate PayloadID if PayloadAttributes are provided (block building request)
	if attr != nil {
		payloadID := s.generateMockPayloadID(attr)
		result.PayloadID = &payloadID

		// Store payload build job for later retrieval
		buildJob := &PayloadBuildJob{
			ID:              payloadID,
			Attributes:      attr,
			ForkchoiceState: state,
			CreatedAt:       uint64(attr.Timestamp),
			Built:           false,
		}
		s.payloadBuilds.Store(payloadID, buildJob)

		s.log.Info("Generated PayloadID for block building", "payloadID", payloadID, "timestamp", attr.Timestamp, "parent", state.HeadBlockHash, "transactions", len(attr.Transactions))
	}

	return result, nil
}

// validateBlockExists checks if a block hash exists in the Sepolia backend
func (s *SyncTester) validateBlockExists(ctx context.Context, blockHash common.Hash, blockType string) (*types.Block, error) {
	block, err := s.elClient.BlockByHash(ctx, blockHash)
	if err != nil {
		s.log.Warn("Block validation failed", "blockType", blockType, "blockHash", blockHash, "error", err)
		return nil, fmt.Errorf("%s block %s not found: %w", blockType, blockHash.Hex(), err)
	}

	s.log.Debug("Block validation successful", "blockType", blockType, "blockHash", blockHash, "blockNumber", block.NumberU64())
	return block, nil
}

// validateForkchoiceChain validates that head >= safe >= finalized in block numbers
func (s *SyncTester) validateForkchoiceChain(headBlock, safeBlock, finalizedBlock *types.Block) error {
	headNum := headBlock.NumberU64()
	safeNum := safeBlock.NumberU64()
	finalizedNum := finalizedBlock.NumberU64()

	if headNum < safeNum {
		return fmt.Errorf("head block number %d is less than safe block number %d", headNum, safeNum)
	}

	if safeNum < finalizedNum {
		return fmt.Errorf("safe block number %d is less than finalized block number %d", safeNum, finalizedNum)
	}

	return nil
}

// forkchoiceInvalidResult returns an INVALID forkchoice result with error details
func (s *SyncTester) forkchoiceInvalidResult(latestValidHash common.Hash, validationError string) *eth.ForkchoiceUpdatedResult {
	return &eth.ForkchoiceUpdatedResult{
		PayloadStatus: eth.PayloadStatusV1{
			Status:          "INVALID",
			LatestValidHash: &latestValidHash,
			ValidationError: &validationError,
		},
	}
}

func (s *SyncTester) NewPayloadV1(ctx context.Context, payload *eth.ExecutionPayload) (*eth.PayloadStatusV1, error) {
	s.log.Warn("NewPayloadV1 requested but only v4 supported for recent blocks", "blockHash", payload.BlockHash, "blockNumber", payload.BlockNumber)
	return &eth.PayloadStatusV1{
		Status:          "INVALID",
		ValidationError: stringPtr("only v4 supported for recent blocks"),
	}, nil
}

func (s *SyncTester) NewPayloadV2(ctx context.Context, payload *eth.ExecutionPayload) (*eth.PayloadStatusV1, error) {
	s.log.Warn("NewPayloadV2 requested but only v4 supported for recent blocks", "blockHash", payload.BlockHash, "blockNumber", payload.BlockNumber)
	return &eth.PayloadStatusV1{
		Status:          "INVALID",
		ValidationError: stringPtr("only v4 supported for recent blocks"),
	}, nil
}

func (s *SyncTester) NewPayloadV3(ctx context.Context, payload *eth.ExecutionPayload, versionedHashes []common.Hash, beaconRoot *common.Hash) (*eth.PayloadStatusV1, error) {
	s.log.Warn("NewPayloadV3 requested but only v4 supported for recent blocks", "blockHash", payload.BlockHash, "blockNumber", payload.BlockNumber)
	return &eth.PayloadStatusV1{
		Status:          "INVALID",
		ValidationError: stringPtr("only v4 supported for recent blocks"),
	}, nil
}

func (s *SyncTester) NewPayloadV4(ctx context.Context, payload *eth.ExecutionPayload, versionedHashes []common.Hash, beaconRoot *common.Hash, executionRequests []hexutil.Bytes) (*eth.PayloadStatusV1, error) {
	return s.validateNewPayloadV4(ctx, payload, versionedHashes, beaconRoot, executionRequests)
}

// validateNewPayloadV4 comprehensively validates v4 payload and associated data
func (s *SyncTester) validateNewPayloadV4(ctx context.Context, payload *eth.ExecutionPayload, versionedHashes []common.Hash, beaconRoot *common.Hash, executionRequests []hexutil.Bytes) (*eth.PayloadStatusV1, error) {
	s.log.Info("Validating NewPayloadV4", "blockHash", payload.BlockHash, "blockNumber", payload.BlockNumber, "parentHash", payload.ParentHash, "versionedHashes", len(versionedHashes), "executionRequests", len(executionRequests))

	// 1. Validate that parent block exists in our backend
	parentBlock, err := s.elClient.BlockByHash(ctx, payload.ParentHash)
	if err != nil {
		s.log.Error("Parent block not found in backend", "parentHash", payload.ParentHash, "error", err)
		return &eth.PayloadStatusV1{
			Status:          "INVALID",
			ValidationError: stringPtr(fmt.Sprintf("parent block %s not found", payload.ParentHash)),
		}, nil
	}

	// 2. Validate block number progression
	expectedBlockNumber := parentBlock.NumberU64() + 1
	actualBlockNumber := uint64(payload.BlockNumber)
	if actualBlockNumber != expectedBlockNumber {
		s.log.Error("Invalid block number progression", "expected", expectedBlockNumber, "actual", actualBlockNumber)
		return &eth.PayloadStatusV1{
			Status:          "INVALID",
			ValidationError: stringPtr(fmt.Sprintf("invalid block number: expected %d, got %d", expectedBlockNumber, actualBlockNumber)),
		}, nil
	}

	// 3. Validate timestamp progression
	if uint64(payload.Timestamp) <= parentBlock.Time() {
		s.log.Error("Invalid timestamp progression", "parentTime", parentBlock.Time(), "payloadTime", payload.Timestamp)
		return &eth.PayloadStatusV1{
			Status:          "INVALID",
			ValidationError: stringPtr("timestamp must be greater than parent"),
		}, nil
	}

	// 4. Check if we built this payload ourselves (validate consistency)
	var ourPayloadID *eth.PayloadID
	s.payloadBuilds.Range(func(key, value interface{}) bool {
		buildJob := value.(*PayloadBuildJob)
		if buildJob.ForkchoiceState.HeadBlockHash == payload.ParentHash &&
			uint64(buildJob.Attributes.Timestamp) == uint64(payload.Timestamp) {
			ourPayloadID = &buildJob.ID
			return false // stop iteration
		}
		return true
	})

	if ourPayloadID != nil {
		s.log.Info("Validating payload we built ourselves", "payloadID", *ourPayloadID, "blockHash", payload.BlockHash)

		// Get our build job and validate consistency
		if buildJobInterface, exists := s.payloadBuilds.Load(*ourPayloadID); exists {
			buildJob := buildJobInterface.(*PayloadBuildJob)

			// Validate that op-node provided same transactions we expected
			if len(payload.Transactions) != len(buildJob.Attributes.Transactions) {
				s.log.Error("Transaction count mismatch", "expected", len(buildJob.Attributes.Transactions), "actual", len(payload.Transactions))
				return &eth.PayloadStatusV1{
					Status:          "INVALID",
					ValidationError: stringPtr("transaction count mismatch with build request"),
				}, nil
			}

			// Validate fee recipient
			if payload.FeeRecipient != buildJob.Attributes.SuggestedFeeRecipient {
				s.log.Error("Fee recipient mismatch", "expected", buildJob.Attributes.SuggestedFeeRecipient, "actual", payload.FeeRecipient)
				return &eth.PayloadStatusV1{
					Status:          "INVALID",
					ValidationError: stringPtr("fee recipient mismatch with build request"),
				}, nil
			}

			s.log.Info("Payload validation successful - matches our build request", "payloadID", *ourPayloadID, "txCount", len(payload.Transactions))
		}
	} else {
		s.log.Info("Validating externally built payload", "blockHash", payload.BlockHash)
	}

	// 5. Log detailed payload info for verification
	s.log.Info("NewPayloadV4 validation complete",
		"blockHash", payload.BlockHash,
		"blockNumber", payload.BlockNumber,
		"parentHash", payload.ParentHash,
		"feeRecipient", payload.FeeRecipient,
		"gasLimit", payload.GasLimit,
		"gasUsed", payload.GasUsed,
		"baseFeePerGas", payload.BaseFeePerGas,
		"txCount", len(payload.Transactions),
		"versionedHashCount", len(versionedHashes),
		"executionRequestCount", len(executionRequests))

	// Return VALID status
	return &eth.PayloadStatusV1{
		Status:          "VALID",
		LatestValidHash: &payload.BlockHash,
	}, nil
}

// validateNewPayload validates payload against Sepolia backend and returns appropriate response
func (s *SyncTester) validateNewPayload(ctx context.Context, payload *eth.ExecutionPayload) (*eth.PayloadStatusV1, error) {
	s.log.Info("Validating NewPayload", "blockHash", payload.BlockHash, "blockNumber", payload.BlockNumber, "parentHash", payload.ParentHash)

	// Validate that the block exists in Sepolia
	block, err := s.elClient.BlockByHash(ctx, payload.BlockHash)
	if err != nil {
		s.log.Warn("NewPayload validation failed - block not found", "blockHash", payload.BlockHash, "error", err)
		return &eth.PayloadStatusV1{
			Status:          "INVALID",
			ValidationError: stringPtr(fmt.Sprintf("block %s not found in chain: %v", payload.BlockHash.Hex(), err)),
		}, nil
	}

	// Validate block number matches
	if block.NumberU64() != uint64(payload.BlockNumber) {
		s.log.Warn("NewPayload validation failed - block number mismatch", "expectedNumber", payload.BlockNumber, "actualNumber", block.NumberU64())
		return &eth.PayloadStatusV1{
			Status:          "INVALID",
			ValidationError: stringPtr(fmt.Sprintf("block number mismatch: expected %d, got %d", payload.BlockNumber, block.NumberU64())),
		}, nil
	}

	// Validate parent hash matches
	if block.ParentHash() != payload.ParentHash {
		s.log.Warn("NewPayload validation failed - parent hash mismatch", "expectedParent", payload.ParentHash, "actualParent", block.ParentHash())
		return &eth.PayloadStatusV1{
			Status:          "INVALID",
			ValidationError: stringPtr(fmt.Sprintf("parent hash mismatch: expected %s, got %s", payload.ParentHash.Hex(), block.ParentHash().Hex())),
		}, nil
	}

	// Optional: Validate parent block exists (ensures chain continuity)
	if payload.ParentHash != (common.Hash{}) { // Skip genesis block
		_, err := s.elClient.BlockByHash(ctx, payload.ParentHash)
		if err != nil {
			s.log.Warn("NewPayload validation failed - parent block not found", "parentHash", payload.ParentHash, "error", err)
			return &eth.PayloadStatusV1{
				Status:          "INVALID",
				ValidationError: stringPtr(fmt.Sprintf("parent block %s not found: %v", payload.ParentHash.Hex(), err)),
			}, nil
		}
	}

	s.log.Info("NewPayload validation successful", "blockHash", payload.BlockHash, "blockNumber", payload.BlockNumber)

	// Note: In progress-driven mode, we don't advance session state here
	// Only ForkchoiceUpdated (after successful derivation) advances progress

	// Return VALID status accepting the payload
	result := &eth.PayloadStatusV1{
		Status:          "VALID",
		LatestValidHash: &payload.BlockHash,
	}

	return result, nil
}

// generateMockPayloadID creates a mock PayloadID based on payload attributes
func (s *SyncTester) generateMockPayloadID(attr *eth.PayloadAttributes) eth.PayloadID {
	// Create a deterministic but unique PayloadID based on timestamp and prevRandao
	// This mimics what a real engine would do for payload building
	hash := fmt.Sprintf("%d-%s", attr.Timestamp, common.Hash(attr.PrevRandao).Hex())
	hashBytes := common.BytesToHash([]byte(hash))

	var out eth.PayloadID
	copy(out[:], hashBytes[:8]) // PayloadID is first 8 bytes
	return out
}

// buildMockPayload builds a mock ExecutionPayload for GetPayloadV1
func (s *SyncTester) buildMockPayload(ctx context.Context, payloadID eth.PayloadID) (*eth.ExecutionPayload, error) {
	// Retrieve the payload build job
	buildJobInterface, exists := s.payloadBuilds.Load(payloadID)
	if !exists {
		s.log.Error("PayloadID not found in build jobs", "payloadID", payloadID)
		return nil, fmt.Errorf("payload %s not found", payloadID.String())
	}

	buildJob := buildJobInterface.(*PayloadBuildJob)
	s.log.Info("Building mock payload", "payloadID", payloadID, "parent", buildJob.ForkchoiceState.HeadBlockHash, "timestamp", buildJob.Attributes.Timestamp)

	// Get parent block for building child
	parentBlock, err := s.elClient.BlockByHash(ctx, buildJob.ForkchoiceState.HeadBlockHash)
	if err != nil {
		return nil, fmt.Errorf("failed to get parent block %s: %w", buildJob.ForkchoiceState.HeadBlockHash, err)
	}

	// Build mock payload based on parent and attributes
	payload := &eth.ExecutionPayload{
		ParentHash:    buildJob.ForkchoiceState.HeadBlockHash,
		FeeRecipient:  buildJob.Attributes.SuggestedFeeRecipient,
		StateRoot:     eth.Bytes32{},  // Mock - would be computed in real engine
		ReceiptsRoot:  eth.Bytes32{},  // Mock - would be computed in real engine
		LogsBloom:     eth.Bytes256{}, // Mock - would be computed in real engine
		PrevRandao:    buildJob.Attributes.PrevRandao,
		BlockNumber:   eth.Uint64Quantity(parentBlock.NumberU64() + 1),
		GasLimit:      eth.Uint64Quantity(*buildJob.Attributes.GasLimit),
		GasUsed:       eth.Uint64Quantity(0), // Mock - no gas used
		Timestamp:     eth.Uint64Quantity(buildJob.Attributes.Timestamp),
		ExtraData:     eth.BytesMax32{},                                                 // Empty extra data
		BaseFeePerGas: eth.Uint256Quantity(*uint256.MustFromBig(parentBlock.BaseFee())), // Inherit from parent
		BlockHash:     common.Hash{},                                                    // Would be computed from payload
		Transactions:  buildJob.Attributes.Transactions,                                 // Use provided transactions
	}

	// Mock block hash generation (deterministic based on payload)
	payload.BlockHash = s.generateMockBlockHash(payload)

	// Mark as built
	buildJob.Built = true
	s.payloadBuilds.Store(payloadID, buildJob)

	s.log.Info("Mock payload built successfully", "payloadID", payloadID, "blockNumber", payload.BlockNumber, "blockHash", payload.BlockHash, "txCount", len(payload.Transactions))
	return payload, nil
}

// buildMockPayloadV4 builds a mock ExecutionPayloadEnvelope for GetPayloadV4
func (s *SyncTester) buildMockPayloadV4(ctx context.Context, payloadID eth.PayloadID) (*eth.ExecutionPayloadEnvelope, error) {
	// First build the basic payload
	payload, err := s.buildMockPayload(ctx, payloadID)
	if err != nil {
		return nil, err
	}

	// Wrap in envelope with additional v4 fields
	envelope := &eth.ExecutionPayloadEnvelope{
		ExecutionPayload: payload,
		// ParentBeaconBlockRoot would be set if available - mock as nil
		ParentBeaconBlockRoot: nil,
	}

	s.log.Info("Mock payload envelope v4 built", "payloadID", payloadID, "blockHash", payload.BlockHash)
	return envelope, nil
}

// generateMockBlockHash creates a deterministic mock block hash
func (s *SyncTester) generateMockBlockHash(payload *eth.ExecutionPayload) common.Hash {
	// Create a simple deterministic hash based on parent + number + timestamp
	data := fmt.Sprintf("%s-%d-%d", payload.ParentHash.Hex(), payload.BlockNumber, payload.Timestamp)
	return common.BytesToHash([]byte(data))
}

// stringPtr returns a pointer to the given string (helper for optional fields)
func stringPtr(s string) *string {
	return &s
}

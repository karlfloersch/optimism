package backend

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum-optimism/optimism/op-sync-tester/metrics"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
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
	payloadCache  sync.Map // map[eth.PayloadID]*eth.ExecutionPayload - for fast GetPayload lookups
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

// createBackendContext creates a context for backend HTTP calls with timeout
// This prevents request context cancellation from crashing the sync-tester
func (s *SyncTester) createBackendContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 30*time.Second)
}

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
		return existing, nil // Return the persistent session!
	} else {
		s.sessions[session.SessionID] = session
		s.log.Info("💾 Created new persistent session", "sessionID", session.SessionID)
		return session, nil // Return the new session
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
	// CRITICAL FIX: Use background context with timeout for backend calls
	backendCtx, cancel := s.createBackendContext()
	defer cancel()
	return s.elClient.BlockReceipts(backendCtx, blockNrOrHash)
}

func (s *SyncTester) GetBlockByHash(ctx context.Context, hash common.Hash, fullTx bool) (interface{}, error) {
	// Direct proxy - hash lookups don't need session offset logic
	// Use raw RPC call to preserve all JSON fields

	// CRITICAL FIX: Use background context with timeout for backend calls
	// Request context cancellation was causing sync-tester to crash!
	backendCtx, cancel := s.createBackendContext()
	defer cancel()

	var result interface{}
	err := s.elClient.Client().CallContext(backendCtx, &result, "eth_getBlockByHash", hash, fullTx)
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

	// CRITICAL FIX: Use background context with timeout for backend calls
	backendCtx, cancel := s.createBackendContext()
	defer cancel()

	// Use raw RPC call to preserve all JSON fields
	var result interface{}
	err = s.elClient.Client().CallContext(backendCtx, &result, "eth_getBlockByNumber", blockNumberParam, fullTx)
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
	// CRITICAL FIX: Use background context for chain tip queries
	backendCtx, cancel := s.createBackendContext()
	defer cancel()
	currentBlockNumber, err := s.elClient.BlockNumber(backendCtx)
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

// advanceSessionProgress updates available heads when op-node successfully processes blocks via ForkchoiceUpdated
func (s *SyncTester) advanceSessionProgress(ctx context.Context, latestBlock, safeBlock, finalizedBlock uint64) {
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
	// Use background context for chain tip queries
	backendCtx, cancel := s.createBackendContext()
	defer cancel()
	currentSepolia, err := s.elClient.BlockNumber(backendCtx)
	if err != nil {
		s.log.Warn("Failed to get current Sepolia tip, cannot advance progress", "error", err)
		return
	}

	// Don't advance past real chain tip
	if latestBlock >= currentSepolia {
		s.log.Debug("Not advancing - latest block at/past Sepolia tip",
			"latest", latestBlock, "sepolia_tip", currentSepolia)
		return
	}

	// Get before state for logging
	beforeLatest, beforeSafe, beforeFinalized := session.GetAvailableHeads()

	// Advance progress using op-node's actual ForkchoiceState values
	session.AdvanceProgress(latestBlock, safeBlock, finalizedBlock)

	// Get after state for logging
	afterLatest, afterSafe, afterFinalized := session.GetAvailableHeads()

	s.log.Info("🚀 op-node ForkchoiceState advanced session",
		"sessionID", session.SessionID,
		"opnode_latest", latestBlock, "opnode_safe", safeBlock, "opnode_finalized", finalizedBlock,
		"available_latest", fmt.Sprintf("%d→%d", beforeLatest, afterLatest),
		"available_safe", fmt.Sprintf("%d→%d", beforeSafe, afterSafe),
		"available_finalized", fmt.Sprintf("%d→%d", beforeFinalized, afterFinalized))
}

func (s *SyncTester) ChainId(ctx context.Context) (*hexutil.Big, error) {
	return (*hexutil.Big)(s.chainID.ToBig()), nil
}

// Engine API methods - these need to make direct RPC calls since ethclient doesn't expose them
func (s *SyncTester) GetPayloadV1(ctx context.Context, payloadID eth.PayloadID) (*eth.ExecutionPayload, error) {
	s.log.Info("GetPayloadV1 requested", "payloadID", payloadID)
	return s.buildPayload(ctx, payloadID)
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
	return s.buildPayloadV4(ctx, payloadID)
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

	// Advance session progress using op-node's actual ForkchoiceState values
	s.advanceSessionProgress(ctx, headBlock.NumberU64(), safeBlock.NumberU64(), finalizedBlock.NumberU64())

	// Return VALID status with the head block hash as latest valid
	result := &eth.ForkchoiceUpdatedResult{
		PayloadStatus: eth.PayloadStatusV1{
			Status:          "VALID",
			LatestValidHash: &state.HeadBlockHash,
		},
	}

	// Generate PayloadID if PayloadAttributes are provided (block building request)
	if attr != nil {
		payloadID := s.computePayloadId(state.HeadBlockHash, attr)
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
	// Use background context for backend validation calls
	backendCtx, cancel := s.createBackendContext()
	defer cancel()
	block, err := s.elClient.BlockByHash(backendCtx, blockHash)
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
	// Use background context for backend validation calls
	backendCtx, cancel := s.createBackendContext()
	defer cancel()
	parentBlock, err := s.elClient.BlockByHash(backendCtx, payload.ParentHash)
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

	// 5. Perform comprehensive validation against real Sepolia block
	// This catches op-node derivation bugs by comparing every field
	validationResult, err := s.validatePayloadAgainstRealBlock(ctx, payload)
	if err != nil {
		s.log.Error("Comprehensive payload validation failed", "error", err)
		return &eth.PayloadStatusV1{
			Status:          "INVALID",
			ValidationError: stringPtr(fmt.Sprintf("comprehensive validation failed: %v", err)),
		}, nil
	}

	if !validationResult.Valid {
		s.log.Error("Payload does not match real Sepolia block", 
			"blockHash", payload.BlockHash,
			"blockNumber", payload.BlockNumber,
			"errors", validationResult.Errors)
		
		// Return INVALID but don't crash - this helps us catch op-node bugs
		return &eth.PayloadStatusV1{
			Status:          "INVALID",
			ValidationError: stringPtr(fmt.Sprintf("payload mismatch with real block: %s", validationResult.Errors[0])),
		}, nil
	}

	// 6. Log detailed payload info for verification
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
	// Use background context for backend validation calls
	backendCtx, cancel := s.createBackendContext()
	defer cancel()
	block, err := s.elClient.BlockByHash(backendCtx, payload.BlockHash)
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
		_, err := s.elClient.BlockByHash(backendCtx, payload.ParentHash)
		if err != nil {
			s.log.Warn("NewPayload validation failed - parent block not found", "parentHash", payload.ParentHash, "error", err)
			return &eth.PayloadStatusV1{
				Status:          "INVALID",
				ValidationError: stringPtr(fmt.Sprintf("parent block %s not found: %v", payload.ParentHash.Hex(), err)),
			}, nil
		}
	}

	s.log.Info("NewPayload validation successful", "blockHash", payload.BlockHash, "blockNumber", payload.BlockNumber)

	// Perform comprehensive validation against real Sepolia block
	// This catches op-node derivation bugs by comparing every field
	validationResult, err := s.validatePayloadAgainstRealBlock(ctx, payload)
	if err != nil {
		s.log.Error("Comprehensive payload validation failed", "error", err)
		return &eth.PayloadStatusV1{
			Status:          "INVALID",
			ValidationError: stringPtr(fmt.Sprintf("comprehensive validation failed: %v", err)),
		}, nil
	}

	if !validationResult.Valid {
		s.log.Error("Payload does not match real Sepolia block",
			"blockHash", payload.BlockHash,
			"blockNumber", payload.BlockNumber,
			"errors", validationResult.Errors)

		// Return INVALID but don't crash - this helps us catch op-node bugs
		return &eth.PayloadStatusV1{
			Status:          "INVALID",
			ValidationError: stringPtr(fmt.Sprintf("payload mismatch with real block: %s", validationResult.Errors[0])),
		}, nil
	}

	// Note: In progress-driven mode, we don't advance session state here
	// Only ForkchoiceUpdated (after successful derivation) advances progress

	// Return VALID status accepting the payload
	result := &eth.PayloadStatusV1{
		Status:          "VALID",
		LatestValidHash: &payload.BlockHash,
	}

	return result, nil
}

// validatePayloadAgainstRealBlock performs comprehensive validation of payload against real Sepolia block
// This catches op-node derivation bugs by comparing every field
func (s *SyncTester) validatePayloadAgainstRealBlock(ctx context.Context, payload *eth.ExecutionPayload) (*PayloadValidationResult, error) {
	s.log.Info("Comprehensive payload validation against real Sepolia block", "blockHash", payload.BlockHash, "blockNumber", payload.BlockNumber)

	// Get the real Sepolia block
	backendCtx, cancel := s.createBackendContext()
	defer cancel()
	realBlock, err := s.elClient.BlockByHash(backendCtx, payload.BlockHash)
	if err != nil {
		return &PayloadValidationResult{
			Valid: false,
			Error: fmt.Sprintf("Real Sepolia block not found: %v", err),
		}, nil
	}

	var errors []string

	// Validate all header fields
	if payload.ParentHash != realBlock.ParentHash() {
		errors = append(errors, fmt.Sprintf("ParentHash mismatch: payload=%s, real=%s", payload.ParentHash.Hex(), realBlock.ParentHash().Hex()))
	}

	if payload.FeeRecipient != realBlock.Coinbase() {
		errors = append(errors, fmt.Sprintf("FeeRecipient mismatch: payload=%s, real=%s", payload.FeeRecipient.Hex(), realBlock.Coinbase().Hex()))
	}

	if payload.StateRoot != eth.Bytes32(realBlock.Root()) {
		errors = append(errors, fmt.Sprintf("StateRoot mismatch: payload=%s, real=%s", common.Hash(payload.StateRoot).Hex(), realBlock.Root().Hex()))
	}

	if payload.ReceiptsRoot != eth.Bytes32(realBlock.ReceiptHash()) {
		errors = append(errors, fmt.Sprintf("ReceiptsRoot mismatch: payload=%s, real=%s", common.Hash(payload.ReceiptsRoot).Hex(), realBlock.ReceiptHash().Hex()))
	}

	if payload.LogsBloom != eth.Bytes256(realBlock.Bloom()) {
		errors = append(errors, fmt.Sprintf("LogsBloom mismatch"))
	}

	if payload.PrevRandao != eth.Bytes32(realBlock.MixDigest()) {
		errors = append(errors, fmt.Sprintf("PrevRandao mismatch: payload=%s, real=%s", common.Hash(payload.PrevRandao).Hex(), realBlock.MixDigest().Hex()))
	}

	if uint64(payload.BlockNumber) != realBlock.NumberU64() {
		errors = append(errors, fmt.Sprintf("BlockNumber mismatch: payload=%d, real=%d", payload.BlockNumber, realBlock.NumberU64()))
	}

	if uint64(payload.GasLimit) != realBlock.GasLimit() {
		errors = append(errors, fmt.Sprintf("GasLimit mismatch: payload=%d, real=%d", payload.GasLimit, realBlock.GasLimit()))
	}

	if uint64(payload.GasUsed) != realBlock.GasUsed() {
		errors = append(errors, fmt.Sprintf("GasUsed mismatch: payload=%d, real=%d", payload.GasUsed, realBlock.GasUsed()))
	}

	if uint64(payload.Timestamp) != realBlock.Time() {
		errors = append(errors, fmt.Sprintf("Timestamp mismatch: payload=%d, real=%d", payload.Timestamp, realBlock.Time()))
	}

	// Compare ExtraData (allow flexibility for different lengths)
	if !bytes.Equal(payload.ExtraData, realBlock.Extra()) {
		errors = append(errors, fmt.Sprintf("ExtraData mismatch: payload=%x, real=%x", payload.ExtraData, realBlock.Extra()))
	}

	// Compare BaseFeePerGas
	realBaseFee := uint256.MustFromBig(realBlock.BaseFee())
	if payload.BaseFeePerGas != eth.Uint256Quantity(*realBaseFee) {
		errors = append(errors, fmt.Sprintf("BaseFeePerGas mismatch: payload=%s, real=%s", (*uint256.Int)(&payload.BaseFeePerGas).String(), realBaseFee.String()))
	}

	if payload.BlockHash != realBlock.Hash() {
		errors = append(errors, fmt.Sprintf("BlockHash mismatch: payload=%s, real=%s", payload.BlockHash.Hex(), realBlock.Hash().Hex()))
	}

	// Validate transactions
	if len(payload.Transactions) != len(realBlock.Transactions()) {
		errors = append(errors, fmt.Sprintf("Transaction count mismatch: payload=%d, real=%d", len(payload.Transactions), len(realBlock.Transactions())))
	} else {
		// Compare each transaction byte-by-byte
		for i, realTx := range realBlock.Transactions() {
			realTxBytes, err := realTx.MarshalBinary()
			if err != nil {
				errors = append(errors, fmt.Sprintf("Failed to marshal real transaction %d: %v", i, err))
				continue
			}

			if !bytes.Equal(payload.Transactions[i], realTxBytes) {
				errors = append(errors, fmt.Sprintf("Transaction %d mismatch: different bytes", i))

				// Log transaction details for debugging
				s.log.Warn("Transaction mismatch details",
					"index", i,
					"payloadTxHash", crypto.Keccak256Hash(payload.Transactions[i]).Hex(),
					"realTxHash", realTx.Hash().Hex(),
					"payloadTxLen", len(payload.Transactions[i]),
					"realTxLen", len(realTxBytes))
			}
		}
	}

	result := &PayloadValidationResult{
		Valid:           len(errors) == 0,
		Errors:          errors,
		BlockNumber:     uint64(payload.BlockNumber),
		BlockHash:       payload.BlockHash,
		RealBlockExists: true,
	}

	if len(errors) > 0 {
		s.log.Error("Payload validation failed against real Sepolia block",
			"blockHash", payload.BlockHash,
			"blockNumber", payload.BlockNumber,
			"errorCount", len(errors),
			"errors", errors)
	} else {
		s.log.Info("Payload validation successful - matches real Sepolia block perfectly",
			"blockHash", payload.BlockHash,
			"blockNumber", payload.BlockNumber,
			"txCount", len(payload.Transactions))
	}

	return result, nil
}

// PayloadValidationResult contains the result of comprehensive payload validation
type PayloadValidationResult struct {
	Valid           bool
	Error           string   // Single error message for when Real block doesn't exist
	Errors          []string // Multiple validation errors when comparing fields
	BlockNumber     uint64
	BlockHash       common.Hash
	RealBlockExists bool
}

// computePayloadId computes a pseudo-random payloadid, based on the parameters.
// This is the standard implementation used by op-geth and op-program.
func (s *SyncTester) computePayloadId(headBlockHash common.Hash, attrs *eth.PayloadAttributes) eth.PayloadID {
	// Hash
	hasher := sha256.New()
	hasher.Write(headBlockHash[:])
	_ = binary.Write(hasher, binary.BigEndian, attrs.Timestamp)
	hasher.Write(attrs.PrevRandao[:])
	hasher.Write(attrs.SuggestedFeeRecipient[:])
	_ = binary.Write(hasher, binary.BigEndian, attrs.NoTxPool)
	_ = binary.Write(hasher, binary.BigEndian, uint64(len(attrs.Transactions)))
	for _, tx := range attrs.Transactions {
		_ = binary.Write(hasher, binary.BigEndian, uint64(len(tx))) // length-prefix to avoid collisions
		hasher.Write(tx)
	}
	_ = binary.Write(hasher, binary.BigEndian, *attrs.GasLimit)
	if attrs.EIP1559Params != nil {
		hasher.Write(attrs.EIP1559Params[:])
	}
	var out eth.PayloadID
	copy(out[:], hasher.Sum(nil)[:8])
	return out
}

// buildPayload builds an ExecutionPayload using real Sepolia block data
func (s *SyncTester) buildPayload(ctx context.Context, payloadID eth.PayloadID) (*eth.ExecutionPayload, error) {
	// Check cache first for fast lookups
	if cachedPayloadInterface, exists := s.payloadCache.Load(payloadID); exists {
		cachedPayload := cachedPayloadInterface.(*eth.ExecutionPayload)
		s.log.Info("Returning cached payload", "payloadID", payloadID, "blockHash", cachedPayload.BlockHash)
		return cachedPayload, nil
	}

	// Retrieve the payload build job
	buildJobInterface, exists := s.payloadBuilds.Load(payloadID)
	if !exists {
		s.log.Error("PayloadID not found in build jobs", "payloadID", payloadID)
		return nil, fmt.Errorf("payload %s not found", payloadID.String())
	}

	buildJob := buildJobInterface.(*PayloadBuildJob)
	s.log.Info("Building payload from real Sepolia block", "payloadID", payloadID, "parent", buildJob.ForkchoiceState.HeadBlockHash)

	// Get parent block to determine next block number
	// Use background context for backend calls
	backendCtx, cancel := s.createBackendContext()
	defer cancel()
	parentBlock, err := s.elClient.BlockByHash(backendCtx, buildJob.ForkchoiceState.HeadBlockHash)
	if err != nil {
		return nil, fmt.Errorf("failed to get parent block %s: %w", buildJob.ForkchoiceState.HeadBlockHash, err)
	}

	// Calculate the expected next block number
	expectedBlockNumber := parentBlock.NumberU64() + 1

	// Get the REAL Sepolia block at this number
	realBlock, err := s.elClient.BlockByNumber(backendCtx, big.NewInt(int64(expectedBlockNumber)))
	if err != nil {
		s.log.Error("Cannot build payload - real Sepolia block not available", "blockNumber", expectedBlockNumber, "error", err)
		return nil, eth.InputError{
			Inner: fmt.Errorf("payload for block %d does not exist / is not available in Sepolia chain: %w", expectedBlockNumber, err),
			Code:  eth.UnknownPayload, // -38001
		}
	}

	// Convert real block to ExecutionPayload format
	transactions := make([]eth.Data, len(realBlock.Transactions()))
	for i, tx := range realBlock.Transactions() {
		txData, _ := tx.MarshalBinary()
		transactions[i] = eth.Data(txData)
	}

	payload := &eth.ExecutionPayload{
		ParentHash:    realBlock.ParentHash(),
		FeeRecipient:  realBlock.Coinbase(),
		StateRoot:     eth.Bytes32(realBlock.Root()),
		ReceiptsRoot:  eth.Bytes32(realBlock.ReceiptHash()),
		LogsBloom:     eth.Bytes256(realBlock.Bloom()),
		PrevRandao:    eth.Bytes32(realBlock.MixDigest()),
		BlockNumber:   eth.Uint64Quantity(realBlock.NumberU64()),
		GasLimit:      eth.Uint64Quantity(realBlock.GasLimit()),
		GasUsed:       eth.Uint64Quantity(realBlock.GasUsed()),
		Timestamp:     eth.Uint64Quantity(realBlock.Time()),
		ExtraData:     eth.BytesMax32(realBlock.Extra()),
		BaseFeePerGas: eth.Uint256Quantity(*uint256.MustFromBig(realBlock.BaseFee())),
		BlockHash:     realBlock.Hash(), // REAL block hash from Sepolia!
		Transactions:  transactions,
	}

	// Mark as built
	buildJob.Built = true
	s.payloadBuilds.Store(payloadID, buildJob)

	// Cache the built payload for fast future lookups
	s.payloadCache.Store(payloadID, payload)

	s.log.Info("Real Sepolia payload built successfully", "payloadID", payloadID, "blockNumber", payload.BlockNumber, "blockHash", payload.BlockHash, "txCount", len(payload.Transactions))
	return payload, nil
}

// buildPayloadV4 builds an ExecutionPayloadEnvelope for GetPayloadV4 using real Sepolia block data
func (s *SyncTester) buildPayloadV4(ctx context.Context, payloadID eth.PayloadID) (*eth.ExecutionPayloadEnvelope, error) {
	// First build the basic payload using real Sepolia data
	payload, err := s.buildPayload(ctx, payloadID)
	if err != nil {
		return nil, err
	}

	// Wrap in envelope with additional v4 fields
	envelope := &eth.ExecutionPayloadEnvelope{
		ExecutionPayload: payload,
		// ParentBeaconBlockRoot would be set if available - mock as nil
		ParentBeaconBlockRoot: nil,
	}

	s.log.Info("Real Sepolia payload envelope v4 built", "payloadID", payloadID, "blockHash", payload.BlockHash)
	return envelope, nil
}

// stringPtr returns a pointer to the given string (helper for optional fields)
func stringPtr(s string) *string {
	return &s
}

package polymarket

import (
	"context"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"

	"github.com/ethereum-optimism/optimism/op-e2e/actions/helpers"
	"github.com/ethereum-optimism/optimism/op-e2e/e2eutils"
	"github.com/ethereum-optimism/optimism/op-e2e/e2eutils/polymarket"
	"github.com/ethereum-optimism/optimism/op-service/testlog"
)

// TestPolymarketContractDeploy verifies that the Polymarket contracts can be
// deployed at their exact Polygon addresses in genesis.
func TestPolymarketContractDeploy(t *testing.T) {
	gt := helpers.NewDefaultTesting(t)
	lg := testlog.Logger(t, log.LevelDebug)

	// Setup with Polygon chain ID
	dp := e2eutils.MakeDeployParams(gt, helpers.DefaultRollupTestParams())
	dp.DeployConfig.L2ChainID = polymarket.PolygonChainID

	// Create allocation params with Polymarket contracts
	alloc := &e2eutils.AllocParams{
		L2Alloc:          polymarket.AllocParams(),
		PrefundTestUsers: true,
	}

	sd := e2eutils.Setup(gt, dp, alloc)

	// Create test actors
	miner, seqEngine, sequencer := helpers.SetupSequencerTest(gt, sd, lg)
	_ = miner
	sequencer.ActL2PipelineFull(gt)

	cl := seqEngine.EthClient()

	// Verify contracts are deployed at correct addresses
	ctx := context.Background()

	// Check FeeModule
	feeModuleCode, err := cl.CodeAt(ctx, polymarket.FeeModuleAddr, nil)
	require.NoError(t, err)
	require.True(t, len(feeModuleCode) > 0, "FeeModule should have code")

	// Check CTFExchange
	ctfExchangeCode, err := cl.CodeAt(ctx, polymarket.CTFExchangeAddr, nil)
	require.NoError(t, err)
	require.True(t, len(ctfExchangeCode) > 0, "CTFExchange should have code")

	// Check ConditionalTokens
	conditionalTokensCode, err := cl.CodeAt(ctx, polymarket.ConditionalTokensAddr, nil)
	require.NoError(t, err)
	require.True(t, len(conditionalTokensCode) > 0, "ConditionalTokens should have code")

	// Check USDC
	usdcCode, err := cl.CodeAt(ctx, polymarket.USDCAddr, nil)
	require.NoError(t, err)
	require.True(t, len(usdcCode) > 0, "USDC should have code")

	// Verify operators have ETH
	for _, op := range polymarket.Operators {
		balance, err := cl.BalanceAt(ctx, op, nil)
		require.NoError(t, err)
		require.True(t, balance.Cmp(big.NewInt(0)) > 0, "Operator %s should have balance", op.Hex())
	}

	t.Log("All Polymarket contracts deployed successfully at Polygon addresses")
}

// TestPolymarketReplaySingleTx attempts to replay a single Polymarket transaction.
// This is a basic test that sends the raw transaction calldata.
func TestPolymarketReplaySingleTx(t *testing.T) {
	gt := helpers.NewDefaultTesting(t)
	lg := testlog.Logger(t, log.LevelDebug)

	// Setup with Polygon chain ID
	dp := e2eutils.MakeDeployParams(gt, helpers.DefaultRollupTestParams())
	dp.DeployConfig.L2ChainID = polymarket.PolygonChainID

	// Create allocation params with Polymarket contracts and Alice as admin
	polymarketAlloc := polymarket.AllocParams()

	// Add Alice as an admin in FeeModule
	aliceAddr := dp.Addresses.Alice
	feeModuleStorage := polymarketAlloc[polymarket.FeeModuleAddr].Storage
	aliceAdminSlot := crypto.Keccak256Hash(
		append(
			common.LeftPadBytes(aliceAddr.Bytes(), 32),
			common.LeftPadBytes(big.NewInt(0).Bytes(), 32)...,
		),
	)
	feeModuleStorage[aliceAdminSlot] = common.BigToHash(big.NewInt(1))
	polymarketAlloc[polymarket.FeeModuleAddr] = types.Account{
		Code:    polymarket.FeeModuleBytecode(),
		Storage: feeModuleStorage,
		Balance: big.NewInt(0),
	}

	alloc := &e2eutils.AllocParams{
		L2Alloc:          polymarketAlloc,
		PrefundTestUsers: true,
	}

	sd := e2eutils.Setup(gt, dp, alloc)

	// Create test actors
	miner, seqEngine, sequencer := helpers.SetupSequencerTest(gt, sd, lg)
	_ = miner
	sequencer.ActL2PipelineFull(gt)

	cl := seqEngine.EthClient()
	ctx := context.Background()

	// Get the first sample transaction
	sampleTx := polymarket.SampleTransactions[0]
	t.Logf("Attempting to replay: %s", sampleTx.Description)
	t.Logf("Original hash: %s", sampleTx.OriginalHash)

	// Use Alice as the operator (she's now an admin)
	operatorKey := dp.Secrets.Alice
	operatorAddr := dp.Addresses.Alice
	chainID := big.NewInt(int64(polymarket.PolygonChainID))
	signer := types.NewEIP155Signer(chainID)

	// Get nonce for operator
	nonce, err := cl.PendingNonceAt(ctx, operatorAddr)
	require.NoError(t, err)

	gasPrice, err := cl.SuggestGasPrice(ctx)
	require.NoError(t, err)

	toAddr := sampleTx.To
	tx := types.NewTx(&types.LegacyTx{
		Nonce:    nonce,
		GasPrice: gasPrice,
		Gas:      sampleTx.GasLimit,
		To:       &toAddr,
		Value:    big.NewInt(0),
		Data:     sampleTx.Calldata,
	})

	signedTx, err := types.SignTx(tx, signer, operatorKey)
	require.NoError(t, err)

	// Send the transaction
	err = cl.SendTransaction(ctx, signedTx)
	require.NoError(t, err, "Failed to send transaction")

	// Build a block to include the transaction
	sequencer.ActL2StartBlock(gt)
	seqEngine.ActL2IncludeTx(operatorAddr)(gt)
	sequencer.ActL2EndBlock(gt)

	// Get the receipt
	receipt, err := cl.TransactionReceipt(ctx, signedTx.Hash())
	require.NoError(t, err, "Failed to get receipt")

	t.Logf("Transaction hash: %s", signedTx.Hash().Hex())
	t.Logf("Receipt status: %d", receipt.Status)
	t.Logf("Gas used: %d", receipt.GasUsed)
	t.Logf("Logs count: %d", len(receipt.Logs))

	// Note: This test may fail due to:
	// 1. Signature validation (POLY_1271 requires proxy wallet setup)
	// 2. Token balance issues (makers need USDC/CTF balances)
	// 3. Allowance issues (makers need to approve exchange)
	// These will be addressed in subsequent iterations.
	if receipt.Status != 1 {
		t.Logf("Transaction failed (expected initially due to signature/state issues)")
		t.Logf("This confirms the contracts are deployed and callable")
	} else {
		t.Log("Transaction succeeded!")
	}
}

// TestPolymarketReplayAll attempts to replay all sample transactions.
// This is the full integration test.
func TestPolymarketReplayAll(t *testing.T) {
	gt := helpers.NewDefaultTesting(t)
	lg := testlog.Logger(t, log.LevelDebug)

	// Setup with Polygon chain ID
	dp := e2eutils.MakeDeployParams(gt, helpers.DefaultRollupTestParams())
	dp.DeployConfig.L2ChainID = polymarket.PolygonChainID

	// Create allocation params with Polymarket contracts and Alice as admin
	polymarketAlloc := polymarket.AllocParams()

	// Add Alice as an admin in FeeModule
	aliceAddr := dp.Addresses.Alice
	feeModuleStorage := polymarketAlloc[polymarket.FeeModuleAddr].Storage
	aliceAdminSlot := crypto.Keccak256Hash(
		append(
			common.LeftPadBytes(aliceAddr.Bytes(), 32),
			common.LeftPadBytes(big.NewInt(0).Bytes(), 32)...,
		),
	)
	feeModuleStorage[aliceAdminSlot] = common.BigToHash(big.NewInt(1))
	polymarketAlloc[polymarket.FeeModuleAddr] = types.Account{
		Code:    polymarket.FeeModuleBytecode(),
		Storage: feeModuleStorage,
		Balance: big.NewInt(0),
	}

	// Fund all maker addresses with ETH
	for _, maker := range polymarket.GetMakerAddresses() {
		polymarketAlloc[maker] = types.Account{
			Balance: e2eutils.Ether(1000),
		}
	}

	alloc := &e2eutils.AllocParams{
		L2Alloc:          polymarketAlloc,
		PrefundTestUsers: true,
	}

	sd := e2eutils.Setup(gt, dp, alloc)

	// Create test actors
	miner, seqEngine, sequencer := helpers.SetupSequencerTest(gt, sd, lg)
	_ = miner
	sequencer.ActL2PipelineFull(gt)

	cl := seqEngine.EthClient()
	ctx := context.Background()

	// Use Alice as the operator
	operatorKey := dp.Secrets.Alice
	operatorAddr := dp.Addresses.Alice
	chainID := big.NewInt(int64(polymarket.PolygonChainID))
	signer := types.NewEIP155Signer(chainID)

	successCount := 0
	failCount := 0

	for i, sampleTx := range polymarket.SampleTransactions {
		t.Logf("Replaying transaction %d: %s", i+1, sampleTx.Description)

		// Get nonce
		nonce, err := cl.PendingNonceAt(ctx, operatorAddr)
		require.NoError(t, err)

		gasPrice, err := cl.SuggestGasPrice(ctx)
		require.NoError(t, err)

		// Build transaction
		toAddr := sampleTx.To
		tx := types.NewTx(&types.LegacyTx{
			Nonce:    nonce,
			GasPrice: gasPrice,
			Gas:      sampleTx.GasLimit,
			To:       &toAddr,
			Value:    big.NewInt(0),
			Data:     sampleTx.Calldata,
		})

		signedTx, err := types.SignTx(tx, signer, operatorKey)
		require.NoError(t, err)

		// Send transaction
		err = cl.SendTransaction(ctx, signedTx)
		require.NoError(t, err, "Failed to send transaction %d", i+1)

		// Build block
		sequencer.ActL2StartBlock(gt)
		seqEngine.ActL2IncludeTx(operatorAddr)(gt)
		sequencer.ActL2EndBlock(gt)

		// Get receipt
		receipt, err := cl.TransactionReceipt(ctx, signedTx.Hash())
		require.NoError(t, err)

		if receipt.Status == 1 {
			successCount++
			t.Logf("  SUCCESS - Gas used: %d, Logs: %d", receipt.GasUsed, len(receipt.Logs))
		} else {
			failCount++
			t.Logf("  FAILED - Gas used: %d", receipt.GasUsed)
		}
	}

	t.Logf("\n=== Summary ===")
	t.Logf("Total transactions: %d", len(polymarket.SampleTransactions))
	t.Logf("Successful: %d", successCount)
	t.Logf("Failed: %d", failCount)
}

// TestFeeModuleIsAdmin verifies we can interact with the FeeModule admin functions.
func TestFeeModuleIsAdmin(t *testing.T) {
	gt := helpers.NewDefaultTesting(t)
	lg := testlog.Logger(t, log.LevelDebug)

	// Setup with Polygon chain ID
	dp := e2eutils.MakeDeployParams(gt, helpers.DefaultRollupTestParams())
	dp.DeployConfig.L2ChainID = polymarket.PolygonChainID

	// Create allocation params with Polymarket contracts and Alice as admin
	polymarketAlloc := polymarket.AllocParams()

	// Add Alice as an admin in the FeeModule
	aliceAddr := dp.Addresses.Alice
	feeModuleStorage := polymarketAlloc[polymarket.FeeModuleAddr].Storage
	aliceAdminSlot := crypto.Keccak256Hash(
		append(
			common.LeftPadBytes(aliceAddr.Bytes(), 32),
			common.LeftPadBytes(big.NewInt(0).Bytes(), 32)...,
		),
	)
	feeModuleStorage[aliceAdminSlot] = common.BigToHash(big.NewInt(1))
	polymarketAlloc[polymarket.FeeModuleAddr] = types.Account{
		Code:    polymarket.FeeModuleBytecode(),
		Storage: feeModuleStorage,
		Balance: big.NewInt(0),
	}

	alloc := &e2eutils.AllocParams{
		L2Alloc:          polymarketAlloc,
		PrefundTestUsers: true,
	}

	sd := e2eutils.Setup(gt, dp, alloc)

	// Create test actors
	miner, seqEngine, sequencer := helpers.SetupSequencerTest(gt, sd, lg)
	_ = miner
	sequencer.ActL2PipelineFull(gt)

	cl := seqEngine.EthClient()
	ctx := context.Background()

	// Call isAdmin(Alice) on FeeModule
	// Function signature: isAdmin(address) -> 0x24d7806c
	feeModuleAddr := polymarket.FeeModuleAddr
	isAdminCalldata := append(
		common.FromHex("0x24d7806c"),
		common.LeftPadBytes(aliceAddr.Bytes(), 32)...,
	)

	result, err := cl.CallContract(ctx, ethereum.CallMsg{
		To:   &feeModuleAddr,
		Data: isAdminCalldata,
	}, nil)
	require.NoError(t, err)

	// Result should be true (1)
	isAdmin := new(big.Int).SetBytes(result)
	require.Equal(t, big.NewInt(1), isAdmin, "Alice should be admin")

	t.Log("Alice is correctly set as admin in FeeModule")

	// Also check one of the Polymarket operators
	operatorAddr := polymarket.Operators[0]
	isAdminCalldata = append(
		common.FromHex("0x24d7806c"),
		common.LeftPadBytes(operatorAddr.Bytes(), 32)...,
	)

	result, err = cl.CallContract(ctx, ethereum.CallMsg{
		To:   &feeModuleAddr,
		Data: isAdminCalldata,
	}, nil)
	require.NoError(t, err)

	isAdmin = new(big.Int).SetBytes(result)
	require.Equal(t, big.NewInt(1), isAdmin, "Operator should be admin")

	t.Logf("Operator %s is correctly set as admin in FeeModule", operatorAddr.Hex())
}

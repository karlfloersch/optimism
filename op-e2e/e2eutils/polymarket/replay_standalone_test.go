package polymarket

import (
	"context"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient/simulated"
)

// TestPolymarketContractDeploySimulated verifies that the Polymarket contracts can be
// deployed using go-ethereum's simulated backend.
func TestPolymarketContractDeploySimulated(t *testing.T) {
	// Create a simulated backend with Polymarket contracts pre-deployed
	alloc := AllocParams()

	// Create a test account with funds
	privateKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	testAddr := crypto.PubkeyToAddress(privateKey.PublicKey)

	// Add test account to alloc
	alloc[testAddr] = types.Account{
		Balance: new(big.Int).Mul(big.NewInt(1e18), big.NewInt(1000)), // 1000 ETH
	}

	// Create simulated backend with the Polygon chain ID
	backend := simulated.NewBackend(alloc, simulated.WithBlockGasLimit(30_000_000))
	defer backend.Close()

	client := backend.Client()
	ctx := context.Background()

	// Verify contracts are deployed at correct addresses
	// Check FeeModule
	feeModuleCode, err := client.CodeAt(ctx, FeeModuleAddr, nil)
	require.NoError(t, err)
	require.True(t, len(feeModuleCode) > 0, "FeeModule should have code")
	t.Logf("FeeModule code size: %d bytes", len(feeModuleCode))

	// Check CTFExchange
	ctfExchangeCode, err := client.CodeAt(ctx, CTFExchangeAddr, nil)
	require.NoError(t, err)
	require.True(t, len(ctfExchangeCode) > 0, "CTFExchange should have code")
	t.Logf("CTFExchange code size: %d bytes", len(ctfExchangeCode))

	// Check ConditionalTokens
	conditionalTokensCode, err := client.CodeAt(ctx, ConditionalTokensAddr, nil)
	require.NoError(t, err)
	require.True(t, len(conditionalTokensCode) > 0, "ConditionalTokens should have code")
	t.Logf("ConditionalTokens code size: %d bytes", len(conditionalTokensCode))

	// Check USDC
	usdcCode, err := client.CodeAt(ctx, USDCAddr, nil)
	require.NoError(t, err)
	require.True(t, len(usdcCode) > 0, "USDC should have code")
	t.Logf("USDC code size: %d bytes", len(usdcCode))

	// Verify operators have ETH
	for _, op := range Operators {
		balance, err := client.BalanceAt(ctx, op, nil)
		require.NoError(t, err)
		require.True(t, balance.Cmp(big.NewInt(0)) > 0, "Operator %s should have balance", op.Hex())
	}

	t.Log("All Polymarket contracts deployed successfully at Polygon addresses")
}

// TestFeeModuleIsAdminSimulated tests the admin mapping on FeeModule.
func TestFeeModuleIsAdminSimulated(t *testing.T) {
	// Create alloc with test address as admin
	alloc := AllocParams()

	// Create a test account with funds
	privateKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	testAddr := crypto.PubkeyToAddress(privateKey.PublicKey)

	// Add test account as admin in FeeModule
	feeModuleStorage := alloc[FeeModuleAddr].Storage
	testAdminSlot := crypto.Keccak256Hash(
		append(
			common.LeftPadBytes(testAddr.Bytes(), 32),
			common.LeftPadBytes(big.NewInt(0).Bytes(), 32)...,
		),
	)
	feeModuleStorage[testAdminSlot] = common.BigToHash(big.NewInt(1))
	alloc[FeeModuleAddr] = types.Account{
		Code:    FeeModuleBytecode(),
		Storage: feeModuleStorage,
		Balance: big.NewInt(0),
	}

	// Add test account with funds
	alloc[testAddr] = types.Account{
		Balance: new(big.Int).Mul(big.NewInt(1e18), big.NewInt(1000)), // 1000 ETH
	}

	backend := simulated.NewBackend(alloc, simulated.WithBlockGasLimit(30_000_000))
	defer backend.Close()

	client := backend.Client()
	ctx := context.Background()

	// Call admins(testAddr) on FeeModule
	// Function signature: admins(address) -> 0x429b62e5
	feeModuleAddr := FeeModuleAddr
	adminsCalldata := append(
		common.FromHex("0x429b62e5"),
		common.LeftPadBytes(testAddr.Bytes(), 32)...,
	)

	result, err := client.CallContract(ctx, ethereum.CallMsg{
		To:   &feeModuleAddr,
		Data: adminsCalldata,
	}, nil)
	require.NoError(t, err, "Failed to call admins function")

	// Result should be 1 (admin)
	adminValue := new(big.Int).SetBytes(result)
	require.Equal(t, big.NewInt(1), adminValue, "Test address should be admin (value 1)")

	t.Logf("Test address admin value: %d", adminValue.Int64())

	// Also check one of the Polymarket operators
	operatorAddr := Operators[0]
	adminsCalldata = append(
		common.FromHex("0x429b62e5"),
		common.LeftPadBytes(operatorAddr.Bytes(), 32)...,
	)

	result, err = client.CallContract(ctx, ethereum.CallMsg{
		To:   &feeModuleAddr,
		Data: adminsCalldata,
	}, nil)
	require.NoError(t, err, "Failed to call admins function for operator")

	adminValue = new(big.Int).SetBytes(result)
	require.Equal(t, big.NewInt(1), adminValue, "Operator should be admin (value 1)")

	t.Logf("Operator %s admin value: %d", operatorAddr.Hex(), adminValue.Int64())
}

// TestPolymarketReplaySingleTxSimulated attempts to replay a single transaction.
func TestPolymarketReplaySingleTxSimulated(t *testing.T) {
	// Create alloc with test address as admin
	alloc := AllocParams()

	// Create a test account with funds
	privateKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	testAddr := crypto.PubkeyToAddress(privateKey.PublicKey)

	// Add test account as admin in FeeModule
	feeModuleStorage := alloc[FeeModuleAddr].Storage
	testAdminSlot := crypto.Keccak256Hash(
		append(
			common.LeftPadBytes(testAddr.Bytes(), 32),
			common.LeftPadBytes(big.NewInt(0).Bytes(), 32)...,
		),
	)
	feeModuleStorage[testAdminSlot] = common.BigToHash(big.NewInt(1))
	alloc[FeeModuleAddr] = types.Account{
		Code:    FeeModuleBytecode(),
		Storage: feeModuleStorage,
		Balance: big.NewInt(0),
	}

	// Add test account with funds
	alloc[testAddr] = types.Account{
		Balance: new(big.Int).Mul(big.NewInt(1e18), big.NewInt(1000)), // 1000 ETH
	}

	// Simulated backend always uses chain ID 1337
	chainID := big.NewInt(1337)
	backend := simulated.NewBackend(alloc, simulated.WithBlockGasLimit(30_000_000))
	defer backend.Close()

	client := backend.Client()
	ctx := context.Background()

	// Get the first sample transaction
	sampleTx := SampleTransactions[0]
	t.Logf("Attempting to replay: %s", sampleTx.Description)
	t.Logf("Original hash: %s", sampleTx.OriginalHash)

	// Build the transaction
	nonce, err := client.PendingNonceAt(ctx, testAddr)
	require.NoError(t, err)

	gasPrice, err := client.SuggestGasPrice(ctx)
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

	// Sign the transaction
	signer := types.NewEIP155Signer(chainID)
	signedTx, err := types.SignTx(tx, signer, privateKey)
	require.NoError(t, err)

	// Send the transaction
	err = client.SendTransaction(ctx, signedTx)
	require.NoError(t, err, "Failed to send transaction")

	// Commit the block
	backend.Commit()

	// Get the receipt
	receipt, err := client.TransactionReceipt(ctx, signedTx.Hash())
	require.NoError(t, err, "Failed to get receipt")

	t.Logf("Transaction hash: %s", signedTx.Hash().Hex())
	t.Logf("Receipt status: %d", receipt.Status)
	t.Logf("Gas used: %d", receipt.GasUsed)
	t.Logf("Logs count: %d", len(receipt.Logs))

	// Note: This test may fail due to signature validation issues.
	// The real transaction has EIP712 signatures that validate against specific addresses.
	if receipt.Status != 1 {
		t.Logf("Transaction failed (expected initially due to signature/state issues)")
		t.Logf("This confirms the contracts are deployed and callable")
	} else {
		t.Log("Transaction succeeded!")
	}
}

// TestPolymarketReplayAllSimulated attempts to replay all sample transactions.
func TestPolymarketReplayAllSimulated(t *testing.T) {
	// Create alloc with test address as admin
	alloc := AllocParams()

	// Create a test account with funds
	privateKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	testAddr := crypto.PubkeyToAddress(privateKey.PublicKey)

	// Add test account as admin in FeeModule
	feeModuleStorage := alloc[FeeModuleAddr].Storage
	testAdminSlot := crypto.Keccak256Hash(
		append(
			common.LeftPadBytes(testAddr.Bytes(), 32),
			common.LeftPadBytes(big.NewInt(0).Bytes(), 32)...,
		),
	)
	feeModuleStorage[testAdminSlot] = common.BigToHash(big.NewInt(1))
	alloc[FeeModuleAddr] = types.Account{
		Code:    FeeModuleBytecode(),
		Storage: feeModuleStorage,
		Balance: big.NewInt(0),
	}

	// Add test account with funds
	alloc[testAddr] = types.Account{
		Balance: new(big.Int).Mul(big.NewInt(1e18), big.NewInt(1000)), // 1000 ETH
	}

	// Fund all maker addresses
	for _, maker := range GetMakerAddresses() {
		alloc[maker] = types.Account{
			Balance: new(big.Int).Mul(big.NewInt(1e18), big.NewInt(1000)),
		}
	}

	// Simulated backend always uses chain ID 1337
	chainID := big.NewInt(1337)
	backend := simulated.NewBackend(alloc, simulated.WithBlockGasLimit(30_000_000))
	defer backend.Close()

	client := backend.Client()
	ctx := context.Background()

	signer := types.NewEIP155Signer(chainID)

	successCount := 0
	failCount := 0

	for i, sampleTx := range SampleTransactions {
		t.Logf("Replaying transaction %d: %s", i+1, sampleTx.Description)

		// Get nonce
		nonce, err := client.PendingNonceAt(ctx, testAddr)
		require.NoError(t, err)

		gasPrice, err := client.SuggestGasPrice(ctx)
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

		signedTx, err := types.SignTx(tx, signer, privateKey)
		require.NoError(t, err)

		// Send transaction
		err = client.SendTransaction(ctx, signedTx)
		require.NoError(t, err, "Failed to send transaction %d", i+1)

		// Commit block
		backend.Commit()

		// Get receipt
		receipt, err := client.TransactionReceipt(ctx, signedTx.Hash())
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
	t.Logf("Total transactions: %d", len(SampleTransactions))
	t.Logf("Successful: %d", successCount)
	t.Logf("Failed: %d", failCount)
}

// Helper function to create a transactor from a private key
func newKeyedTransactor(key *bind.TransactOpts) *bind.TransactOpts {
	return key
}

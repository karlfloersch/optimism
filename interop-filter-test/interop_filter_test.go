package interop_filter_test

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"math/big"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/joho/godotenv"
)

const (
	// Real CrossL2Inbox predeploy address
	CrossL2InboxAddr = "0x4200000000000000000000000000000000000022"
	// Fake address (0x43 prefix instead of 0x42) - should not trigger filtering
	FakeInboxAddr = "0x4300000000000000000000000000000000000022"
)

type ChainConfig struct {
	Name   string
	RPC    string
	Client *ethclient.Client
}

func loadEnv(t *testing.T) {
	// Try to load from .env.local in the op directory
	envPath := "/home/main/op/.env.local"
	if err := godotenv.Load(envPath); err != nil {
		t.Logf("Note: Could not load %s: %v (will use environment variables)", envPath, err)
	}
}

func getPrivateKey(t *testing.T) *ecdsa.PrivateKey {
	pkHex := os.Getenv("TEST_PRIVATE_KEY")
	if pkHex == "" {
		t.Fatal("TEST_PRIVATE_KEY environment variable not set")
	}
	// Remove 0x prefix if present
	pkHex = strings.TrimPrefix(pkHex, "0x")

	privateKey, err := crypto.HexToECDSA(pkHex)
	if err != nil {
		t.Fatalf("Failed to parse private key: %v", err)
	}
	return privateKey
}

func getChains(t *testing.T) []ChainConfig {
	chains := []ChainConfig{
		{Name: "Ethereum Sepolia", RPC: os.Getenv("ETH_SEPOLIA_RPC")},
		{Name: "OP Sepolia", RPC: os.Getenv("OP_SEPOLIA_RPC")},
		{Name: "Unichain Sepolia", RPC: os.Getenv("UNI_SEPOLIA_RPC")},
	}

	for i := range chains {
		if chains[i].RPC == "" {
			t.Logf("WARNING: %s RPC not configured, skipping", chains[i].Name)
			continue
		}
		client, err := ethclient.Dial(chains[i].RPC)
		if err != nil {
			t.Fatalf("Failed to connect to %s: %v", chains[i].Name, err)
		}
		chains[i].Client = client
	}
	return chains
}

// createInteropAccessList creates an access list that looks like an interop message access
func createInteropAccessList(targetAddr common.Address) types.AccessList {
	// Create a fake checksum-style storage key (starts with 0x03 for checksum prefix)
	var storageKey common.Hash
	storageKey[0] = 0x03 // PrefixChecksum
	for i := 1; i < 32; i++ {
		storageKey[i] = byte(i)
	}

	// Create a fake lookup entry (starts with 0x01 for lookup prefix)
	var lookupKey common.Hash
	lookupKey[0] = 0x01 // PrefixLookup
	for i := 1; i < 32; i++ {
		lookupKey[i] = byte(i + 100)
	}

	return types.AccessList{
		{
			Address:     targetAddr,
			StorageKeys: []common.Hash{lookupKey, storageKey},
		},
	}
}

// sendTxWithAccessList sends a self-transfer with an access list and returns the result
func sendTxWithAccessList(
	ctx context.Context,
	client *ethclient.Client,
	privateKey *ecdsa.PrivateKey,
	accessList types.AccessList,
	chainName string,
) (success bool, txHash common.Hash, errMsg string) {
	publicKey := privateKey.Public()
	publicKeyECDSA, ok := publicKey.(*ecdsa.PublicKey)
	if !ok {
		return false, common.Hash{}, "failed to get public key"
	}
	fromAddress := crypto.PubkeyToAddress(*publicKeyECDSA)

	// Get chain ID
	chainID, err := client.ChainID(ctx)
	if err != nil {
		return false, common.Hash{}, fmt.Sprintf("failed to get chain ID: %v", err)
	}

	// Get nonce
	nonce, err := client.PendingNonceAt(ctx, fromAddress)
	if err != nil {
		return false, common.Hash{}, fmt.Sprintf("failed to get nonce: %v", err)
	}

	// Get gas price
	gasPrice, err := client.SuggestGasPrice(ctx)
	if err != nil {
		return false, common.Hash{}, fmt.Sprintf("failed to get gas price: %v", err)
	}

	// Create a self-transfer transaction with access list
	value := big.NewInt(0) // 0 ETH transfer
	gasLimit := uint64(50000)

	tx := types.NewTx(&types.AccessListTx{
		ChainID:    chainID,
		Nonce:      nonce,
		GasPrice:   gasPrice,
		Gas:        gasLimit,
		To:         &fromAddress, // Self-transfer
		Value:      value,
		Data:       nil,
		AccessList: accessList,
	})

	// Sign the transaction
	signedTx, err := types.SignTx(tx, types.NewLondonSigner(chainID), privateKey)
	if err != nil {
		return false, common.Hash{}, fmt.Sprintf("failed to sign tx: %v", err)
	}

	// Send the transaction
	err = client.SendTransaction(ctx, signedTx)
	if err != nil {
		return false, signedTx.Hash(), fmt.Sprintf("tx rejected: %v", err)
	}

	fmt.Printf("  [%s] Transaction sent: %s\n", chainName, signedTx.Hash().Hex())

	// Wait for receipt (with timeout)
	receiptCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	for {
		receipt, err := client.TransactionReceipt(receiptCtx, signedTx.Hash())
		if err == nil {
			if receipt.Status == types.ReceiptStatusSuccessful {
				return true, signedTx.Hash(), ""
			}
			return false, signedTx.Hash(), "transaction reverted"
		}
		select {
		case <-receiptCtx.Done():
			return false, signedTx.Hash(), "timeout waiting for receipt"
		case <-time.After(2 * time.Second):
			// Continue polling
		}
	}
}

func TestInteropFilterBug(t *testing.T) {
	loadEnv(t)

	privateKey := getPrivateKey(t)
	chains := getChains(t)

	ctx := context.Background()

	// Get the wallet address for logging
	publicKey := privateKey.Public()
	publicKeyECDSA := publicKey.(*ecdsa.PublicKey)
	walletAddr := crypto.PubkeyToAddress(*publicKeyECDSA)

	fmt.Println("============================================================")
	fmt.Println("Interop Filter Bug Reproduction Test")
	fmt.Println("============================================================")
	fmt.Printf("Wallet: %s\n\n", walletAddr.Hex())

	// Print balances
	fmt.Println("Checking balances...")
	for _, chain := range chains {
		if chain.Client == nil {
			continue
		}
		balance, err := chain.Client.BalanceAt(ctx, walletAddr, nil)
		if err != nil {
			fmt.Printf("  [%s] Failed to get balance: %v\n", chain.Name, err)
		} else {
			// Convert to ETH
			ethBalance := new(big.Float).Quo(new(big.Float).SetInt(balance), big.NewFloat(1e18))
			fmt.Printf("  [%s] Balance: %s ETH\n", chain.Name, ethBalance.Text('f', 6))
		}
	}
	fmt.Println()

	// ============================================================
	// TEST 1: Access list with FAKE address (0x4300...)
	// Expected: Should succeed on ALL chains
	// ============================================================
	fmt.Println("------------------------------------------------------------")
	fmt.Println("TEST 1: Access list targeting FAKE address (0x4300...)")
	fmt.Println("Expected: Should SUCCEED on all chains")
	fmt.Println("------------------------------------------------------------")

	fakeAddr := common.HexToAddress(FakeInboxAddr)
	fakeAccessList := createInteropAccessList(fakeAddr)

	for _, chain := range chains {
		if chain.Client == nil {
			fmt.Printf("  [%s] SKIPPED (no RPC configured)\n", chain.Name)
			continue
		}

		success, txHash, errMsg := sendTxWithAccessList(ctx, chain.Client, privateKey, fakeAccessList, chain.Name)
		if success {
			fmt.Printf("  [%s] SUCCESS - tx: %s\n", chain.Name, txHash.Hex())
		} else {
			fmt.Printf("  [%s] FAILED - %s (tx: %s)\n", chain.Name, errMsg, txHash.Hex())
		}
	}
	fmt.Println()

	// ============================================================
	// TEST 2: Access list with REAL CrossL2Inbox address (0x4200...22)
	// Expected on Ethereum Sepolia: Should SUCCEED (no interop filter)
	// ============================================================
	fmt.Println("------------------------------------------------------------")
	fmt.Println("TEST 2: Access list targeting REAL CrossL2Inbox (0x4200...22)")
	fmt.Println("Testing on Ethereum Sepolia first (should SUCCEED)")
	fmt.Println("------------------------------------------------------------")

	realAddr := common.HexToAddress(CrossL2InboxAddr)
	realAccessList := createInteropAccessList(realAddr)

	// Find Ethereum Sepolia
	for _, chain := range chains {
		if chain.Name != "Ethereum Sepolia" || chain.Client == nil {
			continue
		}

		success, txHash, errMsg := sendTxWithAccessList(ctx, chain.Client, privateKey, realAccessList, chain.Name)
		if success {
			fmt.Printf("  [%s] SUCCESS - tx: %s\n", chain.Name, txHash.Hex())
		} else {
			fmt.Printf("  [%s] FAILED - %s (tx: %s)\n", chain.Name, errMsg, txHash.Hex())
		}
	}
	fmt.Println()

	// ============================================================
	// TEST 3: Access list with REAL CrossL2Inbox on OP chains
	// Expected: Should FAIL (incorrectly filtered by interop filter)
	// THIS DEMONSTRATES THE BUG
	// ============================================================
	fmt.Println("------------------------------------------------------------")
	fmt.Println("TEST 3: Access list targeting REAL CrossL2Inbox (0x4200...22)")
	fmt.Println("Testing on OP Sepolia and Unichain Sepolia")
	fmt.Println("Expected: Should FAIL (BUG: interop filter incorrectly enabled)")
	fmt.Println("------------------------------------------------------------")

	for _, chain := range chains {
		if chain.Name == "Ethereum Sepolia" || chain.Client == nil {
			continue
		}

		success, txHash, errMsg := sendTxWithAccessList(ctx, chain.Client, privateKey, realAccessList, chain.Name)
		if success {
			fmt.Printf("  [%s] SUCCESS - tx: %s\n", chain.Name, txHash.Hex())
			fmt.Printf("           ^ This is UNEXPECTED if the bug exists\n")
		} else {
			fmt.Printf("  [%s] FAILED - %s\n", chain.Name, errMsg)
			if strings.Contains(errMsg, "filtered") || strings.Contains(errMsg, "rejected") {
				fmt.Printf("           ^ This CONFIRMS the bug - tx was incorrectly filtered\n")
			}
		}
	}

	fmt.Println()
	fmt.Println("============================================================")
	fmt.Println("Test Complete")
	fmt.Println("============================================================")
}

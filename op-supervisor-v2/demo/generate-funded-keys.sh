#!/usr/bin/env bash

set -euo pipefail

# Load environment variables
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1090
source "$SCRIPT_DIR/variables.sh"

# Configuration
NUM_KEYS=$1
FUNDING_AMOUNT="10000000000000000000" # 10 ETH in wei
KEYS_FILE="$SCRIPT_DIR/funded-keys.txt"

echo "=== Generating and Funding $NUM_KEYS Private Keys ==="

# Verify required variables are set
if [ -z "${CHAIN_2151908_RPC:-}" ] || [ -z "${CHAIN_2151909_RPC:-}" ] || [ -z "${DEFAULT_PRIV_KEY:-}" ]; then
    echo "ERROR: Required environment variables not set. Make sure Docker containers are running." >&2
    exit 1
fi

echo "Configuration:"
echo "  Chain 901 (2151908) RPC: $CHAIN_2151908_RPC"
echo "  Chain 902 (2151909) RPC: $CHAIN_2151909_RPC"
echo "  Funding Amount: 10 ETH per account per chain"
echo "  Output File: $KEYS_FILE"
echo ""

# Change to op-up directory to access Go modules
cd "$SCRIPT_DIR/../../../karls-op/op-up" || {
    echo "ERROR: Could not find op-up directory" >&2
    exit 1
}

# Create temporary Go program to generate keys and fund accounts
cat > temp_keygen.go << 'EOF'
package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/rand"
	"fmt"
	"math/big"
	"os"
	"strconv"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
)

func main() {
	if len(os.Args) != 6 {
		fmt.Fprintf(os.Stderr, "Usage: %s <num_keys> <funding_amount_wei> <funder_private_key> <rpc1> <rpc2>\n", os.Args[0])
		os.Exit(1)
	}

	numKeys, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Fprintf(os.Stderr, "Invalid number of keys: %v\n", err)
		os.Exit(1)
	}

	fundingAmount := new(big.Int)
	fundingAmount.SetString(os.Args[2], 10)

	funderPrivKey, err := crypto.HexToECDSA(os.Args[3][2:]) // Remove 0x prefix
	if err != nil {
		fmt.Fprintf(os.Stderr, "Invalid funder private key: %v\n", err)
		os.Exit(1)
	}

	rpc1 := os.Args[4]
	rpc2 := os.Args[5]

	// Connect to both chains
	client1, err := ethclient.Dial(rpc1)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect to chain 1: %v\n", err)
		os.Exit(1)
	}
	defer client1.Close()

	client2, err := ethclient.Dial(rpc2)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect to chain 2: %v\n", err)
		os.Exit(1)
	}
	defer client2.Close()

	// Get chain IDs
	chainID1, err := client1.ChainID(context.Background())
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to get chain ID 1: %v\n", err)
		os.Exit(1)
	}

	chainID2, err := client2.ChainID(context.Background())
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to get chain ID 2: %v\n", err)
		os.Exit(1)
	}

	funderAddr := crypto.PubkeyToAddress(funderPrivKey.PublicKey)

	fmt.Printf("# Generated and funded private keys\n")
	fmt.Printf("# Chain 1: %s (%s)\n", chainID1.String(), rpc1)
	fmt.Printf("# Chain 2: %s (%s)\n", chainID2.String(), rpc2)
	fmt.Printf("# Funder: %s\n", funderAddr.Hex())
	fmt.Printf("# Generated: %s\n", time.Now().Format(time.RFC3339))
	fmt.Printf("#\n")
	fmt.Printf("# Format: private_key,address\n")

	for i := 0; i < numKeys; i++ {
		// Generate new private key
		privKey, err := ecdsa.GenerateKey(crypto.S256(), rand.Reader)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to generate key %d: %v\n", i, err)
			continue
		}

		address := crypto.PubkeyToAddress(privKey.PublicKey)
		privKeyHex := fmt.Sprintf("0x%x", crypto.FromECDSA(privKey))

		// Fund on both chains
		success1 := fundAccount(client1, chainID1, funderPrivKey, address, fundingAmount)
		success2 := fundAccount(client2, chainID2, funderPrivKey, address, fundingAmount)

		if success1 && success2 {
			fmt.Printf("%s,%s\n", privKeyHex, address.Hex())
		} else {
			fmt.Fprintf(os.Stderr, "Failed to fund account %d (%s) - Chain1: %t, Chain2: %t\n", i, address.Hex(), success1, success2)
		}
	}
}

func fundAccount(client *ethclient.Client, chainID *big.Int, funderPrivKey *ecdsa.PrivateKey, recipient common.Address, amount *big.Int) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	funderAddr := crypto.PubkeyToAddress(funderPrivKey.PublicKey)

	// Get nonce
	nonce, err := client.PendingNonceAt(ctx, funderAddr)
	if err != nil {
		return false
	}

	// Get gas price
	gasPrice, err := client.SuggestGasPrice(ctx)
	if err != nil {
		return false
	}

	// Create transaction
	tx := types.NewTransaction(nonce, recipient, amount, 21000, gasPrice, nil)

	// Sign transaction
	signedTx, err := types.SignTx(tx, types.NewEIP155Signer(chainID), funderPrivKey)
	if err != nil {
		return false
	}

	// Send transaction
	err = client.SendTransaction(ctx, signedTx)
	if err != nil {
		return false
	}

	// Wait for confirmation (optional - comment out for speed)
	// receipt, err := bind.WaitMined(ctx, client, signedTx)
	// return err == nil && receipt.Status == types.ReceiptStatusSuccessful

	return true
}
EOF

echo "Generating $NUM_KEYS private keys and funding them..."
echo "This may take several minutes..."
echo ""

# Run the key generation and funding
go run temp_keygen.go "$NUM_KEYS" "$FUNDING_AMOUNT" "$DEFAULT_PRIV_KEY" "$CHAIN_2151908_RPC" "$CHAIN_2151909_RPC" > "$KEYS_FILE"

# Clean up temporary file
rm -f temp_keygen.go

# Count successful keys
SUCCESSFUL_KEYS=$(grep -c "^0x" "$KEYS_FILE" || true)

echo "=== Key Generation Complete ==="
echo "Successfully generated and funded: $SUCCESSFUL_KEYS/$NUM_KEYS keys"
echo "Keys saved to: $KEYS_FILE"
echo ""
echo "File format:"
echo "  private_key,address"
echo ""
echo "Usage example:"
echo "  # Read a random key"
echo "  KEY=\$(tail -n +8 \"$KEYS_FILE\" | shuf -n 1)"
echo "  PRIV_KEY=\$(echo \"\$KEY\" | cut -d, -f1)"
echo "  ADDRESS=\$(echo \"\$KEY\" | cut -d, -f2)"
echo ""

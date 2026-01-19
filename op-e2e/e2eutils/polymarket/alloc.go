package polymarket

import (
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
)

// AllocParams returns the L2 genesis allocation for Polymarket contracts.
// This sets up the contracts at their exact Polygon addresses with the necessary
// initial storage state for replaying transactions.
func AllocParams() types.GenesisAlloc {
	alloc := make(types.GenesisAlloc)

	// FeeModule at exact Polygon address
	alloc[FeeModuleAddr] = types.Account{
		Code:    FeeModuleBytecode(),
		Storage: feeModuleStorage(),
		Balance: big.NewInt(0),
	}

	// CTFExchange at exact Polygon address
	alloc[CTFExchangeAddr] = types.Account{
		Code:    CTFExchangeBytecode(),
		Storage: ctfExchangeStorage(),
		Balance: big.NewInt(0),
	}

	// ConditionalTokens at exact Polygon address
	alloc[ConditionalTokensAddr] = types.Account{
		Code:    ConditionalTokensBytecode(),
		Storage: conditionalTokensStorage(),
		Balance: big.NewInt(0),
	}

	// USDC mock at exact Polygon address
	alloc[USDCAddr] = types.Account{
		Code:    USDCBytecode(),
		Storage: usdcStorage(),
		Balance: big.NewInt(0),
	}

	// Fund operators with ETH for gas
	for _, op := range Operators {
		alloc[op] = types.Account{
			Balance: big.NewInt(0).Mul(big.NewInt(1e18), big.NewInt(1000)), // 1000 ETH
		}
	}

	return alloc
}

// feeModuleStorage returns initial storage for the FeeModule contract.
// The FeeModule has:
// - mapping(address => uint256) admins at slot 0
func feeModuleStorage() map[common.Hash]common.Hash {
	storage := make(map[common.Hash]common.Hash)

	// Set operators as admins (admins mapping at slot 0)
	for _, op := range Operators {
		slot := mappingSlot(op, 0)
		storage[slot] = common.BigToHash(big.NewInt(1))
	}

	return storage
}

// ctfExchangeStorage returns initial storage for the CTFExchange contract.
// The CTFExchange has:
// - mapping(address => uint256) admins at slot 0
// - mapping(address => uint256) operators at slot 1
// - mapping(address => uint256) nonces at slot 2
// - mapping(bytes32 => OrderStatus) orderStatus at slot 3
// - mapping(uint256 => TokenInfo) registry at slot 4
// - bool paused at slot 5
// - bytes32 domainSeparator at slot 6 (computed based on chain ID and contract address)
// - address proxyFactory at slot 7
// - address safeFactory at slot 8
func ctfExchangeStorage() map[common.Hash]common.Hash {
	storage := make(map[common.Hash]common.Hash)

	// Set FeeModule as an operator (operators mapping at slot 1)
	operatorSlot := mappingSlot(FeeModuleAddr, 1)
	storage[operatorSlot] = common.BigToHash(big.NewInt(1))

	// Also set operators as admins (admins mapping at slot 0)
	for _, op := range Operators {
		slot := mappingSlot(op, 0)
		storage[slot] = common.BigToHash(big.NewInt(1))
	}

	// paused = false (slot 5), default is already 0

	// domainSeparator (slot 6) - this is computed from:
	// keccak256(abi.encode(
	//     keccak256("EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"),
	//     keccak256("CTFExchange"),
	//     keccak256("1"),
	//     chainId,
	//     address(this)
	// ))
	// For Polygon (chain ID 137), this is precomputed:
	domainSeparator := computeDomainSeparator(PolygonChainID, CTFExchangeAddr)
	storage[common.BigToHash(big.NewInt(6))] = domainSeparator

	// proxyFactory (slot 7)
	storage[common.BigToHash(big.NewInt(7))] = common.BytesToHash(ProxyFactoryAddr.Bytes())

	// safeFactory (slot 8)
	storage[common.BigToHash(big.NewInt(8))] = common.BytesToHash(SafeFactoryAddr.Bytes())

	return storage
}

// conditionalTokensStorage returns initial storage for the ConditionalTokens contract.
// We need to set up token balances for the makers involved in our sample transactions.
func conditionalTokensStorage() map[common.Hash]common.Hash {
	storage := make(map[common.Hash]common.Hash)
	// Will be populated with specific token balances for test participants
	return storage
}

// usdcStorage returns initial storage for the USDC mock contract.
// We need to set up balances and allowances for the test participants.
func usdcStorage() map[common.Hash]common.Hash {
	storage := make(map[common.Hash]common.Hash)
	// Will be populated with specific balances for test participants
	return storage
}

// mappingSlot computes the storage slot for a mapping(address => ...) at the given base slot.
func mappingSlot(key common.Address, baseSlot uint64) common.Hash {
	// slot = keccak256(abi.encode(key, baseSlot))
	keyPadded := common.LeftPadBytes(key.Bytes(), 32)
	slotPadded := common.LeftPadBytes(big.NewInt(int64(baseSlot)).Bytes(), 32)
	data := append(keyPadded, slotPadded...)
	return crypto.Keccak256Hash(data)
}

// mappingSlotUint256 computes the storage slot for a mapping(uint256 => ...) at the given base slot.
func mappingSlotUint256(key *big.Int, baseSlot uint64) common.Hash {
	keyPadded := common.LeftPadBytes(key.Bytes(), 32)
	slotPadded := common.LeftPadBytes(big.NewInt(int64(baseSlot)).Bytes(), 32)
	data := append(keyPadded, slotPadded...)
	return crypto.Keccak256Hash(data)
}

// computeDomainSeparator computes the EIP712 domain separator for CTFExchange.
func computeDomainSeparator(chainID uint64, contractAddr common.Address) common.Hash {
	// EIP712Domain type hash
	domainTypeHash := crypto.Keccak256Hash([]byte("EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"))
	nameHash := crypto.Keccak256Hash([]byte("CTFExchange"))
	versionHash := crypto.Keccak256Hash([]byte("1"))

	// Encode the domain
	var data []byte
	data = append(data, domainTypeHash.Bytes()...)
	data = append(data, nameHash.Bytes()...)
	data = append(data, versionHash.Bytes()...)
	data = append(data, common.LeftPadBytes(big.NewInt(int64(chainID)).Bytes(), 32)...)
	data = append(data, common.LeftPadBytes(contractAddr.Bytes(), 32)...)

	return crypto.Keccak256Hash(data)
}

package polymarket

import (
	_ "embed"
	"strings"

	"github.com/ethereum/go-ethereum/common"
)

// Polymarket contract addresses on Polygon mainnet
var (
	// FeeModule is the operator contract that calls CTFExchange
	FeeModuleAddr = common.HexToAddress("0xE3f18aCc55091e2c48d883fc8C8413319d4Ab7b0")

	// CTFExchange is the main exchange contract
	CTFExchangeAddr = common.HexToAddress("0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E")

	// ConditionalTokens is the ERC1155 token contract
	ConditionalTokensAddr = common.HexToAddress("0x4D97DCd97eC945f40cF65F87097ACe5EA0476045")

	// USDC is the collateral token on Polygon
	USDCAddr = common.HexToAddress("0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174")

	// Operator addresses that send matchOrders transactions
	Operators = []common.Address{
		common.HexToAddress("0x56269807A5bbE30126E61B5D9bC9B1C060123663"),
		common.HexToAddress("0xAE5D7147Ed7C1c312cd2762613442Ee0f2C3124D"),
		common.HexToAddress("0xDa7f6906C9B6917762b8A35862f113ae9426017F"),
	}

	// ProxyFactory is the proxy wallet factory (used for signature validation)
	ProxyFactoryAddr = common.HexToAddress("0xaB45c5A4B0c941a2F231C04C3f49182e1A254052")

	// SafeFactory is the Gnosis Safe factory (used for signature validation)
	SafeFactoryAddr = common.HexToAddress("0xa6B71E26C5e0845f74c812102Ca7114b6a896AB2")
)

// Bytecode for deployed contracts (fetched from Polygon mainnet)
//
//go:embed feemodule.bytecode
var feeModuleBytecodeHex string

//go:embed ctfexchange.bytecode
var ctfExchangeBytecodeHex string

//go:embed conditionaltokens.bytecode
var conditionalTokensBytecodeHex string

//go:embed usdc.bytecode
var usdcBytecodeHex string

// FeeModuleBytecode returns the deployed bytecode
func FeeModuleBytecode() []byte {
	return common.FromHex(strings.TrimSpace(feeModuleBytecodeHex))
}

// CTFExchangeBytecode returns the deployed bytecode
func CTFExchangeBytecode() []byte {
	return common.FromHex(strings.TrimSpace(ctfExchangeBytecodeHex))
}

// ConditionalTokensBytecode returns the deployed bytecode
func ConditionalTokensBytecode() []byte {
	return common.FromHex(strings.TrimSpace(conditionalTokensBytecodeHex))
}

// USDCBytecode returns the deployed bytecode
func USDCBytecode() []byte {
	return common.FromHex(strings.TrimSpace(usdcBytecodeHex))
}

// PolygonChainID is the chain ID for Polygon mainnet
const PolygonChainID = 137

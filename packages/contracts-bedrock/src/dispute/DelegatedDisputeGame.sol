// SPDX-License-Identifier: MIT
pragma solidity 0.8.15;

// Libraries
import { Clone } from "@solady/utils/Clone.sol";
import { GameType, GameStatus, Claim, Timestamp, Hash } from "src/dispute/lib/Types.sol";
import { Types } from "src/libraries/Types.sol";
import { Hashing } from "src/libraries/Hashing.sol";
import { RLPReader } from "src/libraries/rlp/RLPReader.sol";

// Interfaces
import { IDisputeGame } from "interfaces/dispute/IDisputeGame.sol";
import { IAnchorStateRegistry } from "interfaces/dispute/IAnchorStateRegistry.sol";
import { ISuperFaultDisputeGame } from "interfaces/dispute/ISuperFaultDisputeGame.sol";

/// @title DelegatedDisputeGame
/// @notice A minimal dispute game that delegates verification to a SuperFaultDisputeGame.
///         This enables per-chain dispute games that share economic security through a single super game.
///         The DelegatedDisputeGame has:
///         - No bisection/claims - no internal dispute resolution
///         - No bonds - all economic security is in the SuperGame
///         - Delegated verification - status and resolvedAt come from SuperGame
///         - Block number verification - proves l2BlockNumber matches the output root
/// @dev Uses standard DisputeGameFactory.create() with extended extraData.
///      extraData format: abi.encodePacked(
///          uint256 l2BlockNumber,
///          address superGame,
///          uint256 chainId,
///          bytes32 outputRootProof.version,
///          bytes32 outputRootProof.stateRoot,
///          bytes32 outputRootProof.messagePasserStorageRoot,
///          bytes32 outputRootProof.latestBlockhash,
///          bytes headerRLP
///      )
///
///      CWIA Layout (from factory):
///      ┌─────────────┬────────────────────────────────────────────────┐
///      │ Byte Range  │ Description                                    │
///      ├─────────────┼────────────────────────────────────────────────┤
///      │ [0, 20)     │ Game creator address                           │
///      │ [20, 52)    │ Root claim (this chain's output root)          │
///      │ [52, 84)    │ L1 head hash at creation time                  │
///      │ [84, 116)   │ L2 block number (viem compatible extraData)    │
///      │ [116, 136)  │ Super game address                             │
///      │ [136, 168)  │ Chain ID                                       │
///      │ [168, 200)  │ OutputRootProof.version                        │
///      │ [200, 232)  │ OutputRootProof.stateRoot                      │
///      │ [232, 264)  │ OutputRootProof.messagePasserStorageRoot       │
///      │ [264, 296)  │ OutputRootProof.latestBlockhash                │
///      │ [296, ...)  │ headerRLP (variable length)                    │
///      └─────────────┴────────────────────────────────────────────────┘
contract DelegatedDisputeGame is Clone, IDisputeGame {
    ////////////////////////////////////////////////////////////////
    //                       SIZE CONSTANTS                       //
    ////////////////////////////////////////////////////////////////

    /// @notice Size of an address in bytes.
    uint256 internal constant SIZE_ADDRESS = 20;

    /// @notice Size of a bytes32 in bytes.
    uint256 internal constant SIZE_BYTES32 = 32;

    /// @notice Size of the OutputRootProof struct (4 * bytes32).
    uint256 internal constant SIZE_OUTPUT_ROOT_PROOF = 4 * SIZE_BYTES32;

    ////////////////////////////////////////////////////////////////
    //                      OFFSET CONSTANTS                      //
    ////////////////////////////////////////////////////////////////
    // Offsets are chained: OFFSET_B = OFFSET_A + SIZE_A
    // This ensures changes propagate correctly.

    /// @notice CWIA offset for game creator address.
    uint256 internal constant OFFSET_GAME_CREATOR = 0;

    /// @notice CWIA offset for root claim.
    uint256 internal constant OFFSET_ROOT_CLAIM = OFFSET_GAME_CREATOR + SIZE_ADDRESS; // 20

    /// @notice CWIA offset for L1 head hash.
    uint256 internal constant OFFSET_L1_HEAD = OFFSET_ROOT_CLAIM + SIZE_BYTES32; // 52

    /// @notice CWIA offset for L2 block number (start of extraData).
    uint256 internal constant OFFSET_L2_BLOCK_NUMBER = OFFSET_L1_HEAD + SIZE_BYTES32; // 84

    /// @notice CWIA offset for super game address.
    uint256 internal constant OFFSET_SUPER_GAME = OFFSET_L2_BLOCK_NUMBER + SIZE_BYTES32; // 116

    /// @notice CWIA offset for chain ID.
    uint256 internal constant OFFSET_CHAIN_ID = OFFSET_SUPER_GAME + SIZE_ADDRESS; // 136

    /// @notice CWIA offset for OutputRootProof.version.
    uint256 internal constant OFFSET_PROOF_VERSION = OFFSET_CHAIN_ID + SIZE_BYTES32; // 168

    /// @notice CWIA offset for OutputRootProof.stateRoot.
    uint256 internal constant OFFSET_PROOF_STATE_ROOT = OFFSET_PROOF_VERSION + SIZE_BYTES32; // 200

    /// @notice CWIA offset for OutputRootProof.messagePasserStorageRoot.
    uint256 internal constant OFFSET_PROOF_MSG_PASSER_ROOT = OFFSET_PROOF_STATE_ROOT + SIZE_BYTES32; // 232

    /// @notice CWIA offset for OutputRootProof.latestBlockhash.
    uint256 internal constant OFFSET_PROOF_BLOCK_HASH = OFFSET_PROOF_MSG_PASSER_ROOT + SIZE_BYTES32; // 264

    /// @notice CWIA offset for headerRLP (variable length, must be last).
    uint256 internal constant OFFSET_HEADER_RLP = OFFSET_PROOF_BLOCK_HASH + SIZE_BYTES32; // 296

    /// @notice The index of the block number in an RLP-encoded block header.
    /// @dev Consensus encoding reference:
    ///      https://github.com/paradigmxyz/reth/blob/5f82993c23164ce8ccdc7bf3ae5085205383a5c8/crates/primitives/src/header.rs#L368
    uint256 internal constant HEADER_BLOCK_NUMBER_INDEX = 8;

    ////////////////////////////////////////////////////////////////
    //                         CONSTANTS                          //
    ////////////////////////////////////////////////////////////////

    /// @notice Semantic version.
    /// @custom:semver 1.0.0
    string public constant VERSION = "1.0.0";

    ////////////////////////////////////////////////////////////////
    //                         IMMUTABLES                         //
    ////////////////////////////////////////////////////////////////

    /// @notice The game type ID for this delegated dispute game.
    GameType internal immutable GAME_TYPE;

    /// @notice The anchor state registry for this chain's validation.
    IAnchorStateRegistry internal immutable ANCHOR_STATE_REGISTRY;

    /// @notice The superchain-level anchor state registry for SuperGame validation.
    IAnchorStateRegistry internal immutable SUPERCHAIN_REGISTRY;

    ////////////////////////////////////////////////////////////////
    //                        MUTABLE STATE                       //
    ////////////////////////////////////////////////////////////////

    /// @notice Timestamp of the game's creation.
    Timestamp public createdAt;

    /// @notice Flag to track initialization.
    bool internal initialized;

    /// @notice Whether the respected game type was set when this game was created.
    bool internal respectedGameTypeWhenCreated;

    ////////////////////////////////////////////////////////////////
    //                           ERRORS                           //
    ////////////////////////////////////////////////////////////////

    /// @notice Thrown when the game has already been initialized.
    error AlreadyInitialized();

    /// @notice Thrown when bonds are sent (this game accepts no bonds).
    error NoBondsAccepted();

    /// @notice Thrown when the root claim doesn't match the SuperGame's claim for this chain.
    error RootClaimMismatch();

    /// @notice Thrown when the game is not in progress (for resolve).
    error GameNotInProgress();

    /// @notice Thrown when the output root proof doesn't hash to the root claim.
    error InvalidOutputRootProof();

    /// @notice Thrown when the header RLP doesn't hash to the block hash in the output root proof.
    error InvalidHeaderRLP();

    /// @notice Thrown when the block number in the header doesn't match l2BlockNumber.
    error L2BlockNumberMismatch();

    /// @notice Thrown when the SuperGame address is zero.
    error InvalidSuperGame();

    /// @notice Thrown when the SuperGame is not registered in the superchain registry.
    error SuperGameNotRegistered();

    /// @notice Thrown when the SuperGame's registry doesn't match SUPERCHAIN_REGISTRY.
    error SuperchainRegistryMismatch();

    ////////////////////////////////////////////////////////////////
    //                        CONSTRUCTOR                         //
    ////////////////////////////////////////////////////////////////

    /// @param _gameType The game type ID for this delegated dispute game.
    /// @param _anchorStateRegistry The anchor state registry for this chain.
    /// @param _superchainRegistry The superchain-level anchor state registry for SuperGame validation.
    constructor(GameType _gameType, IAnchorStateRegistry _anchorStateRegistry, IAnchorStateRegistry _superchainRegistry) {
        GAME_TYPE = _gameType;
        ANCHOR_STATE_REGISTRY = _anchorStateRegistry;
        SUPERCHAIN_REGISTRY = _superchainRegistry;
    }

    ////////////////////////////////////////////////////////////////
    //                       INITIALIZATION                       //
    ////////////////////////////////////////////////////////////////

    /// @notice Initializes the delegated dispute game.
    /// @dev No bonds are accepted. Verification is delegated to the SuperGame.
    ///      Block number is verified against the output root proof and header RLP.
    function initialize() external payable {
        // INVARIANT: The game must not have been initialized.
        if (initialized) revert AlreadyInitialized();

        // INVARIANT: No bonds are accepted for delegated games.
        if (msg.value != 0) revert NoBondsAccepted();

        // Mark as initialized before external calls (CEI pattern).
        initialized = true;

        // Fetch the super game and verify configuration.
        ISuperFaultDisputeGame superGameContract = superGame();

        // INVARIANT: SuperGame address must not be zero.
        if (address(superGameContract) == address(0)) revert InvalidSuperGame();

        // INVARIANT: SuperGame must be registered in the superchain registry.
        // This prevents fake SuperGames from being used.
        if (!SUPERCHAIN_REGISTRY.isGameRegistered(IDisputeGame(address(superGameContract)))) {
            revert SuperGameNotRegistered();
        }

        // INVARIANT: SuperGame's registry must match SUPERCHAIN_REGISTRY.
        // This ensures consistency between initialize() and isGameProper() validation.
        if (address(superGameContract.anchorStateRegistry()) != address(SUPERCHAIN_REGISTRY)) {
            revert SuperchainRegistryMismatch();
        }

        // Verify the root claim matches what the SuperGame has for this chain.
        Claim expectedRoot = superGameContract.rootClaimByChainId(chainId());
        if (rootClaim().raw() != expectedRoot.raw()) revert RootClaimMismatch();

        // Verify the block number matches the output root proof and header RLP.
        _verifyBlockNumber();

        // Check if the respected game type matches at creation time.
        respectedGameTypeWhenCreated = ANCHOR_STATE_REGISTRY.respectedGameType().raw() == GAME_TYPE.raw();

        // Set the creation timestamp.
        createdAt = Timestamp.wrap(uint64(block.timestamp));
    }

    ////////////////////////////////////////////////////////////////
    //                    DELEGATION METHODS                      //
    ////////////////////////////////////////////////////////////////

    /// @notice Returns the current status of the game.
    /// @dev Delegates to the SuperGame's status.
    /// @return status_ The current status of the game.
    function status() external view returns (GameStatus status_) {
        status_ = superGame().status();
    }

    /// @notice Returns the timestamp when the game was resolved.
    /// @dev Delegates to the SuperGame's resolvedAt.
    /// @return resolvedAt_ The timestamp when the game was resolved.
    function resolvedAt() external view returns (Timestamp resolvedAt_) {
        resolvedAt_ = superGame().resolvedAt();
    }

    /// @notice Resolves the game. For delegated games, this is a no-op that returns the SuperGame's status.
    /// @return status_ The current status of the game.
    function resolve() external returns (GameStatus status_) {
        // Get status from super game
        status_ = superGame().status();

        // If not yet resolved, revert
        if (status_ == GameStatus.IN_PROGRESS) revert GameNotInProgress();

        emit Resolved(status_);
    }

    /// @notice Returns the L2 sequence number (block number for per-chain games).
    /// @dev Decodes the L2 block number from the first 32 bytes of extraData.
    /// @return l2SequenceNumber_ The L2 block number.
    function l2SequenceNumber() external pure returns (uint256 l2SequenceNumber_) {
        l2SequenceNumber_ = _l2BlockNumber();
    }

    ////////////////////////////////////////////////////////////////
    //                     IMMUTABLE GETTERS                      //
    ////////////////////////////////////////////////////////////////

    /// @notice Returns the game type.
    /// @return gameType_ The game type.
    function gameType() public view returns (GameType gameType_) {
        gameType_ = GAME_TYPE;
    }

    /// @notice Returns the game creator address.
    /// @return creator_ The game creator address.
    function gameCreator() public pure returns (address creator_) {
        creator_ = _getArgAddress(OFFSET_GAME_CREATOR);
    }

    /// @notice Returns the root claim (this chain's output root).
    /// @return rootClaim_ The root claim.
    function rootClaim() public pure returns (Claim rootClaim_) {
        rootClaim_ = Claim.wrap(_getArgBytes32(OFFSET_ROOT_CLAIM));
    }

    /// @notice Returns the L1 head hash at game creation.
    /// @return l1Head_ The L1 head hash.
    function l1Head() public pure returns (Hash l1Head_) {
        l1Head_ = Hash.wrap(_getArgBytes32(OFFSET_L1_HEAD));
    }

    /// @notice Returns the extra data (full extraData including proof data).
    /// @dev Returns the complete CWIA extraData section for factory lookups.
    ///      Use l2SequenceNumber() for viem-compatible block number access.
    /// @return extraData_ The full extra data bytes.
    function extraData() public pure returns (bytes memory extraData_) {
        // Calculate the full extraData length from CWIA args.
        // Total immutable args length = msg.data.length - offset - 2 byte CWIA suffix
        uint256 offset = _getImmutableArgsOffset();
        // Guard against underflow when called on implementation (not clone)
        if (msg.data.length <= offset + 2 + OFFSET_L2_BLOCK_NUMBER) {
            return extraData_;
        }
        uint256 immutableArgsLength = msg.data.length - offset - 2;
        // extraData starts at OFFSET_L2_BLOCK_NUMBER (84), so its length is total - 84
        uint256 extraDataLength = immutableArgsLength - OFFSET_L2_BLOCK_NUMBER;
        extraData_ = _getArgBytes(OFFSET_L2_BLOCK_NUMBER, extraDataLength);
    }

    /// @notice Returns the anchor state registry.
    /// @return registry_ The anchor state registry.
    function anchorStateRegistry() public view returns (IAnchorStateRegistry registry_) {
        registry_ = ANCHOR_STATE_REGISTRY;
    }

    /// @notice Returns the superchain-level anchor state registry.
    /// @return registry_ The superchain anchor state registry.
    function superchainRegistry() public view returns (IAnchorStateRegistry registry_) {
        registry_ = SUPERCHAIN_REGISTRY;
    }

    /// @notice Returns the super game this delegated game is linked to.
    /// @return superGame_ The super fault dispute game.
    function superGame() public pure returns (ISuperFaultDisputeGame superGame_) {
        superGame_ = ISuperFaultDisputeGame(_getArgAddress(OFFSET_SUPER_GAME));
    }

    /// @notice Returns the chain ID this delegated game is for.
    /// @return chainId_ The chain ID.
    function chainId() public pure returns (uint256 chainId_) {
        chainId_ = _getArgUint256(OFFSET_CHAIN_ID);
    }

    /// @notice Returns the game data (gameType, rootClaim, extraData).
    /// @dev extraData returns the full CWIA extra data for factory lookups.
    ///      Use l2SequenceNumber() for viem-compatible block number access.
    /// @return gameType_ The game type.
    /// @return rootClaim_ The root claim.
    /// @return extraData_ The full extra data.
    function gameData() external view returns (GameType gameType_, Claim rootClaim_, bytes memory extraData_) {
        gameType_ = gameType();
        rootClaim_ = rootClaim();
        extraData_ = extraData();
    }

    /// @notice Returns whether the respected game type was set when this game was created.
    /// @return respected_ True if the game type was respected at creation.
    function wasRespectedGameTypeWhenCreated() external view returns (bool respected_) {
        respected_ = respectedGameTypeWhenCreated;
    }

    /// @notice Returns the semantic version string.
    /// @return version_ The semantic version string.
    function version() external pure returns (string memory version_) {
        version_ = VERSION;
    }

    ////////////////////////////////////////////////////////////////
    //                    INTERNAL HELPERS                        //
    ////////////////////////////////////////////////////////////////

    /// @notice Returns the L2 block number from CWIA args.
    /// @return l2BlockNumber_ The L2 block number.
    function _l2BlockNumber() internal pure returns (uint256 l2BlockNumber_) {
        l2BlockNumber_ = _getArgUint256(OFFSET_L2_BLOCK_NUMBER);
    }

    /// @notice Returns the OutputRootProof from CWIA args.
    /// @return proof_ The output root proof.
    function _getOutputRootProof() internal pure returns (Types.OutputRootProof memory proof_) {
        proof_ = Types.OutputRootProof({
            version: _getArgBytes32(OFFSET_PROOF_VERSION),
            stateRoot: _getArgBytes32(OFFSET_PROOF_STATE_ROOT),
            messagePasserStorageRoot: _getArgBytes32(OFFSET_PROOF_MSG_PASSER_ROOT),
            latestBlockhash: _getArgBytes32(OFFSET_PROOF_BLOCK_HASH)
        });
    }

    /// @notice Returns the length of the headerRLP from CWIA args.
    /// @return length_ The length of the headerRLP in bytes.
    function _headerRLPLength() internal pure returns (uint256 length_) {
        // Total immutable args length = msg.data.length - offset - 2 byte CWIA suffix
        uint256 offset = _getImmutableArgsOffset();
        // Guard against underflow when called on implementation (not clone)
        if (msg.data.length <= offset + 2 + OFFSET_HEADER_RLP) {
            return 0;
        }
        uint256 immutableArgsLength = msg.data.length - offset - 2;
        length_ = immutableArgsLength - OFFSET_HEADER_RLP;
    }

    /// @notice Returns the headerRLP from CWIA args.
    /// @return headerRLP_ The RLP-encoded block header.
    function _getHeaderRLP() internal pure returns (bytes memory headerRLP_) {
        headerRLP_ = _getArgBytes(OFFSET_HEADER_RLP, _headerRLPLength());
    }

    /// @notice Verifies that the l2BlockNumber matches the block number in the output root.
    /// @dev Reverts if the output root proof is invalid, header RLP is invalid, or block number mismatches.
    function _verifyBlockNumber() internal pure {
        // Get the output root proof from CWIA args.
        Types.OutputRootProof memory proof = _getOutputRootProof();

        // Verify: hash(proof) == rootClaim
        if (Hashing.hashOutputRootProof(proof) != rootClaim().raw()) {
            revert InvalidOutputRootProof();
        }

        // Get the header RLP from CWIA args.
        bytes memory headerRLP = _getHeaderRLP();

        // Verify: keccak256(headerRLP) == proof.latestBlockhash
        if (keccak256(headerRLP) != proof.latestBlockhash) {
            revert InvalidHeaderRLP();
        }

        // Decode the header RLP to extract the block number.
        // Block number is at index 8 in the RLP-encoded block header.
        RLPReader.RLPItem[] memory headerContents = RLPReader.readList(RLPReader.toRLPItem(headerRLP));

        // Sanity check the header has enough elements.
        if (headerContents.length <= HEADER_BLOCK_NUMBER_INDEX) revert InvalidHeaderRLP();

        bytes memory rawBlockNumber = RLPReader.readBytes(headerContents[HEADER_BLOCK_NUMBER_INDEX]);

        // Sanity check the block number string length.
        if (rawBlockNumber.length > 32) revert InvalidHeaderRLP();

        // Convert the raw, left-aligned block number to a uint256.
        // SAFETY: The length of `rawBlockNumber` is checked above to ensure it is at most 32 bytes.
        uint256 blockNumber;
        assembly {
            blockNumber := shr(shl(0x03, sub(0x20, mload(rawBlockNumber))), mload(add(rawBlockNumber, 0x20)))
        }

        // Verify: extracted block number == l2BlockNumber from extraData
        if (blockNumber != _l2BlockNumber()) {
            revert L2BlockNumberMismatch();
        }
    }
}

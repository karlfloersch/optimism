// SPDX-License-Identifier: MIT
pragma solidity ^0.8.15;

// Testing
import { SuperFaultDisputeGame_TestInit, BaseSuperFaultDisputeGame_TestInit } from "test/dispute/SuperFaultDisputeGame.t.sol";

// Libraries
import { GameType, GameStatus, Claim, Timestamp, Hash, VMStatus, VMStatuses } from "src/dispute/lib/Types.sol";
import { Types } from "src/libraries/Types.sol";
import { Hashing } from "src/libraries/Hashing.sol";
import { RLPWriter } from "src/libraries/rlp/RLPWriter.sol";

// Contracts
import { DelegatedDisputeGame } from "src/dispute/DelegatedDisputeGame.sol";

// Interfaces
import { IDisputeGame } from "interfaces/dispute/IDisputeGame.sol";
import { ISuperFaultDisputeGame } from "interfaces/dispute/ISuperFaultDisputeGame.sol";

/// @title DelegatedDisputeGame_Test
/// @notice Tests for the DelegatedDisputeGame contract using standard DisputeGameFactory.
///         Sets up SuperGame with real OutputRootProof hashes (no mocking).
contract DelegatedDisputeGame_Test is BaseSuperFaultDisputeGame_TestInit {
    /// @dev The game type for delegated games.
    GameType internal constant DELEGATED_GAME_TYPE = GameType.wrap(100);

    /// @dev The DelegatedDisputeGame implementation.
    DelegatedDisputeGame internal delegatedGameImpl;

    /// @dev A created DelegatedDisputeGame proxy.
    DelegatedDisputeGame internal delegatedGameProxy;

    /// @dev Stored proof data for chain 5.
    Types.OutputRootProof internal proofChain5;
    bytes internal headerRLPChain5;
    bytes32 internal outputRootChain5;

    /// @dev Stored proof data for chain 6.
    Types.OutputRootProof internal proofChain6;
    bytes internal headerRLPChain6;
    bytes32 internal outputRootChain6;

    /// @dev The root claim of the game.
    Claim internal ROOT_CLAIM;

    /// @dev The super root preimage of the game.
    Types.SuperRootProof SUPER_ROOT_PROOF;

    /// @dev The preimage of the absolute prestate claim.
    bytes internal absolutePrestateData;

    /// @dev The absolute prestate of the trace.
    Claim internal absolutePrestate;

    /// @dev A valid l2SequenceNumber that comes after the current anchor root block.
    uint256 validl2SequenceNumber;

    function setUp() public override {
        absolutePrestateData = abi.encode(0);
        absolutePrestate = _changeClaimStatus(Claim.wrap(keccak256(absolutePrestateData)), VMStatuses.UNFINISHED);

        super.setUp();

        // Get the actual anchor roots.
        (, uint256 l2Seqno) = anchorStateRegistry.getAnchorRoot();
        validl2SequenceNumber = l2Seqno + 1;

        // Generate real OutputRootProofs for each chain.
        // These will be the actual root claims stored in the SuperGame.
        (proofChain5, outputRootChain5, headerRLPChain5) =
            _generateOutputRootProof(bytes32(uint256(5)), bytes32(uint256(5000)), abi.encodePacked(uint256(5000)));

        (proofChain6, outputRootChain6, headerRLPChain6) =
            _generateOutputRootProof(bytes32(uint256(6)), bytes32(uint256(6000)), abi.encodePacked(uint256(6000)));

        // Build SUPER_ROOT_PROOF with real OutputRootProof hashes.
        SUPER_ROOT_PROOF.version = bytes1(uint8(1));
        SUPER_ROOT_PROOF.timestamp = uint64(validl2SequenceNumber);
        SUPER_ROOT_PROOF.outputRoots.push(Types.OutputRootWithChainId({ chainId: 5, root: outputRootChain5 }));
        SUPER_ROOT_PROOF.outputRoots.push(Types.OutputRootWithChainId({ chainId: 6, root: outputRootChain6 }));
        ROOT_CLAIM = Claim.wrap(Hashing.hashSuperRootProof(SUPER_ROOT_PROOF));

        // Initialize the SuperGame with real output roots.
        init({ _rootClaim: ROOT_CLAIM, _absolutePrestate: absolutePrestate, _super: SUPER_ROOT_PROOF });

        // Deploy the DelegatedDisputeGame implementation with constructor args.
        delegatedGameImpl = new DelegatedDisputeGame(DELEGATED_GAME_TYPE, anchorStateRegistry);

        // Register the implementation with the factory.
        // Transfer ownership to this test contract first (already done in parent setUp).
        disputeGameFactory.setImplementation(DELEGATED_GAME_TYPE, delegatedGameImpl);

        // Set init bond to 0 for delegated games (no bonds).
        disputeGameFactory.setInitBond(DELEGATED_GAME_TYPE, 0);
    }

    /// @notice Helper to change the VM status byte of a claim.
    function _changeClaimStatus(Claim _claim, VMStatus _status) internal pure returns (Claim out_) {
        assembly {
            out_ := or(and(not(shl(248, 0xFF)), _claim), shl(248, _status))
        }
    }

    /// @notice Helper to generate a mock RLP encoded header and output root proof.
    /// @param _storageRoot Arbitrary storage root for the proof.
    /// @param _withdrawalRoot Arbitrary withdrawal root for the proof.
    /// @param _l2BlockNumber The L2 block number to encode in the header.
    /// @return proof_ The output root proof.
    /// @return root_ The output root (hash of the proof).
    /// @return rlp_ The RLP-encoded block header.
    function _generateOutputRootProof(
        bytes32 _storageRoot,
        bytes32 _withdrawalRoot,
        bytes memory _l2BlockNumber
    )
        internal
        pure
        returns (Types.OutputRootProof memory proof_, bytes32 root_, bytes memory rlp_)
    {
        // L2 Block header (minimal mock with block number at index 8)
        bytes[] memory rawHeaderRLP = new bytes[](9);
        rawHeaderRLP[0] = hex"83FACADE";
        rawHeaderRLP[1] = hex"83FACADE";
        rawHeaderRLP[2] = hex"83FACADE";
        rawHeaderRLP[3] = hex"83FACADE";
        rawHeaderRLP[4] = hex"83FACADE";
        rawHeaderRLP[5] = hex"83FACADE";
        rawHeaderRLP[6] = hex"83FACADE";
        rawHeaderRLP[7] = hex"83FACADE";
        rawHeaderRLP[8] = RLPWriter.writeBytes(_l2BlockNumber);
        rlp_ = RLPWriter.writeList(rawHeaderRLP);

        // Output root proof
        proof_ = Types.OutputRootProof({
            version: 0,
            stateRoot: _storageRoot,
            messagePasserStorageRoot: _withdrawalRoot,
            latestBlockhash: keccak256(rlp_)
        });
        root_ = Hashing.hashOutputRootProof(proof_);
    }

    /// @notice Helper to create extended extraData for delegated games with block number proof.
    /// @param _l2BlockNumber The L2 block number.
    /// @param _superGame The SuperFaultDisputeGame address.
    /// @param _chainId The chain ID.
    /// @param _proof The output root proof.
    /// @param _headerRLP The RLP-encoded block header.
    /// @return extraData_ The encoded extra data.
    function _createExtendedExtraData(
        uint256 _l2BlockNumber,
        address _superGame,
        uint256 _chainId,
        Types.OutputRootProof memory _proof,
        bytes memory _headerRLP
    )
        internal
        pure
        returns (bytes memory extraData_)
    {
        // Format: abi.encodePacked(
        //   l2BlockNumber,    // 32 bytes
        //   superGame,        // 20 bytes
        //   chainId,          // 32 bytes
        //   proof.version,    // 32 bytes
        //   proof.stateRoot,  // 32 bytes
        //   proof.messagePasserStorageRoot, // 32 bytes
        //   proof.latestBlockhash, // 32 bytes
        //   headerRLP         // variable
        // )
        extraData_ = abi.encodePacked(
            _l2BlockNumber,
            _superGame,
            _chainId,
            _proof.version,
            _proof.stateRoot,
            _proof.messagePasserStorageRoot,
            _proof.latestBlockhash,
            _headerRLP
        );
    }

    /// @notice Helper to create a delegated game for chain 5 (uses stored proof data).
    /// @param _l2BlockNumber The L2 block number to claim.
    /// @return game_ The created delegated game.
    function _createDelegatedGameChain5(uint256 _l2BlockNumber)
        internal
        returns (DelegatedDisputeGame game_)
    {
        // For chain 5, the block number in the proof is 5000.
        // If _l2BlockNumber matches 5000, it will pass verification.
        // Otherwise it will fail with L2BlockNumberMismatch.
        bytes memory extraData = _createExtendedExtraData(
            _l2BlockNumber,
            address(gameProxy),
            5,
            proofChain5,
            headerRLPChain5
        );

        game_ = DelegatedDisputeGame(
            payable(address(disputeGameFactory.create(DELEGATED_GAME_TYPE, Claim.wrap(outputRootChain5), extraData)))
        );
    }

    /// @notice Helper to create a delegated game for chain 6 (uses stored proof data).
    /// @param _l2BlockNumber The L2 block number to claim.
    /// @return game_ The created delegated game.
    function _createDelegatedGameChain6(uint256 _l2BlockNumber)
        internal
        returns (DelegatedDisputeGame game_)
    {
        bytes memory extraData = _createExtendedExtraData(
            _l2BlockNumber,
            address(gameProxy),
            6,
            proofChain6,
            headerRLPChain6
        );

        game_ = DelegatedDisputeGame(
            payable(address(disputeGameFactory.create(DELEGATED_GAME_TYPE, Claim.wrap(outputRootChain6), extraData)))
        );
    }

    /// @notice Tests that the implementation is correctly initialized.
    function test_implementation_initialization_succeeds() public view {
        assertEq(delegatedGameImpl.gameType().raw(), DELEGATED_GAME_TYPE.raw());
        assertEq(address(delegatedGameImpl.anchorStateRegistry()), address(anchorStateRegistry));
    }

    /// @notice Tests that a delegated game can be created successfully via standard factory.
    function test_create_succeeds() public {
        // Create delegated game for chain 5 with matching block number (5000).
        delegatedGameProxy = _createDelegatedGameChain5(5000);

        // Verify the game was created.
        assertTrue(address(delegatedGameProxy) != address(0));

        // Verify the game's properties.
        assertEq(delegatedGameProxy.gameType().raw(), DELEGATED_GAME_TYPE.raw());
        assertEq(delegatedGameProxy.rootClaim().raw(), outputRootChain5);
        assertEq(delegatedGameProxy.chainId(), 5);
        assertEq(address(delegatedGameProxy.superGame()), address(gameProxy));
        assertEq(address(delegatedGameProxy.anchorStateRegistry()), address(anchorStateRegistry));

        // Verify extraData contains full proof data (first 32 bytes are l2BlockNumber).
        bytes memory fullExtraData = delegatedGameProxy.extraData();
        uint256 decodedBlockNumber;
        assembly {
            decodedBlockNumber := mload(add(fullExtraData, 32))
        }
        assertEq(decodedBlockNumber, 5000);

        // Verify l2SequenceNumber matches.
        assertEq(delegatedGameProxy.l2SequenceNumber(), 5000);
    }

    /// @notice Tests that create reverts if root claim doesn't match SuperGame.
    function test_create_rootClaimMismatch_reverts() public {
        uint256 chainId = 5;
        uint256 l2BlockNumber = 5000;

        // Use a different root claim than what SuperGame returns.
        Claim wrongRootClaim = Claim.wrap(bytes32(uint256(12345)));

        bytes memory extraData = _createExtendedExtraData(
            l2BlockNumber, address(gameProxy), chainId, proofChain5, headerRLPChain5
        );

        vm.expectRevert(DelegatedDisputeGame.RootClaimMismatch.selector);
        disputeGameFactory.create(DELEGATED_GAME_TYPE, wrongRootClaim, extraData);
    }

    /// @notice Tests that create reverts if bonds are sent.
    /// @dev The factory rejects with IncorrectBondAmount since init bond is 0.
    function test_create_withBonds_reverts() public {
        bytes memory extraData = _createExtendedExtraData(
            5000, address(gameProxy), 5, proofChain5, headerRLPChain5
        );

        // Factory rejects because init bond is 0, so any value is incorrect
        vm.expectRevert(abi.encodeWithSignature("IncorrectBondAmount()"));
        disputeGameFactory.create{ value: 1 ether }(DELEGATED_GAME_TYPE, Claim.wrap(outputRootChain5), extraData);
    }

    /// @notice Tests that status is delegated to SuperGame.
    function test_status_delegatesToSuperGame() public {
        delegatedGameProxy = _createDelegatedGameChain5(5000);
        // Status should match SuperGame's status.
        assertEq(uint256(delegatedGameProxy.status()), uint256(gameProxy.status()));
    }

    /// @notice Tests that resolvedAt is delegated to SuperGame.
    function test_resolvedAt_delegatesToSuperGame() public {
        delegatedGameProxy = _createDelegatedGameChain5(5000);
        // resolvedAt should match SuperGame's resolvedAt.
        assertEq(delegatedGameProxy.resolvedAt().raw(), gameProxy.resolvedAt().raw());
    }

    /// @notice Tests that l2SequenceNumber returns the block number from extraData.
    function test_l2SequenceNumber_returnsBlockNumber() public {
        delegatedGameProxy = _createDelegatedGameChain5(5000);
        assertEq(delegatedGameProxy.l2SequenceNumber(), 5000);
    }

    /// @notice Tests that gameData returns correct values.
    function test_gameData_returnsCorrectValues() public {
        delegatedGameProxy = _createDelegatedGameChain5(5000);

        (GameType gameType_, Claim rootClaim_, bytes memory extraData_) = delegatedGameProxy.gameData();

        assertEq(gameType_.raw(), DELEGATED_GAME_TYPE.raw());
        assertEq(rootClaim_.raw(), outputRootChain5);
        // extraData first 32 bytes are l2BlockNumber (use l2SequenceNumber for viem compatibility)
        uint256 decodedBlockNumber;
        assembly {
            decodedBlockNumber := mload(add(extraData_, 32))
        }
        assertEq(decodedBlockNumber, 5000);
    }

    /// @notice Tests that the factory's game lookup functions work correctly.
    function test_gameAtIndex_succeeds() public {
        uint256 gameCountBefore = disputeGameFactory.gameCount();

        DelegatedDisputeGame proxy = _createDelegatedGameChain5(5000);

        (GameType gameType_, Timestamp timestamp_, IDisputeGame proxy_) = disputeGameFactory.gameAtIndex(gameCountBefore);

        assertEq(gameType_.raw(), DELEGATED_GAME_TYPE.raw());
        assertTrue(timestamp_.raw() > 0);
        assertEq(address(proxy_), address(proxy));
    }

    /// @notice Tests that games() lookup works correctly.
    function test_games_succeeds() public {
        bytes memory extraData = _createExtendedExtraData(
            5000, address(gameProxy), 5, proofChain5, headerRLPChain5
        );

        IDisputeGame proxy = disputeGameFactory.create(DELEGATED_GAME_TYPE, Claim.wrap(outputRootChain5), extraData);

        // Look up by game parameters
        (IDisputeGame proxy_, Timestamp timestamp_) =
            disputeGameFactory.games(DELEGATED_GAME_TYPE, Claim.wrap(outputRootChain5), extraData);

        assertTrue(timestamp_.raw() > 0);
        assertEq(address(proxy_), address(proxy));
    }

    /// @notice Tests that gameCount is incremented correctly.
    function test_gameCount_increments() public {
        uint256 gameCountBefore = disputeGameFactory.gameCount();

        // Create first delegated game for chain 5.
        _createDelegatedGameChain5(5000);
        assertEq(disputeGameFactory.gameCount(), gameCountBefore + 1);

        // Create second delegated game for chain 6.
        _createDelegatedGameChain6(6000);
        assertEq(disputeGameFactory.gameCount(), gameCountBefore + 2);
    }

    /// @notice Tests that createdAt is set correctly.
    function test_createdAt_isSetCorrectly() public {
        uint256 expectedTimestamp = block.timestamp;
        delegatedGameProxy = _createDelegatedGameChain5(5000);
        assertEq(delegatedGameProxy.createdAt().raw(), expectedTimestamp);
    }

    /// @notice Tests that wasRespectedGameTypeWhenCreated returns correct value.
    function test_wasRespectedGameTypeWhenCreated_returnsCorrectValue() public {
        // The delegated game type is not the respected type (SUPER_CANNON is), so this should be false.
        delegatedGameProxy = _createDelegatedGameChain5(5000);
        assertFalse(delegatedGameProxy.wasRespectedGameTypeWhenCreated());
    }

    /// @notice Tests that version returns the correct string.
    function test_version_returnsCorrectString() public view {
        assertEq(delegatedGameImpl.version(), "1.0.0");
    }

    /// @notice Tests that gameCreator returns the correct address.
    function test_gameCreator_returnsCorrectAddress() public {
        delegatedGameProxy = _createDelegatedGameChain5(5000);
        // gameCreator should be the caller of create() (this test contract)
        assertEq(delegatedGameProxy.gameCreator(), address(this));
    }

    /// @notice Tests that l1Head returns a non-zero value.
    function test_l1Head_returnsNonZero() public {
        delegatedGameProxy = _createDelegatedGameChain5(5000);
        // l1Head should be the parent block hash
        assertTrue(delegatedGameProxy.l1Head().raw() != bytes32(0));
    }

    /// @notice Tests creating multiple delegated games for different chains from same SuperGame.
    function test_multipleChains_succeeds() public {
        // Create delegated game for chain 5
        DelegatedDisputeGame game5 = _createDelegatedGameChain5(5000);

        // Create delegated game for chain 6
        DelegatedDisputeGame game6 = _createDelegatedGameChain6(6000);

        // Verify both games point to same SuperGame
        assertEq(address(game5.superGame()), address(gameProxy));
        assertEq(address(game6.superGame()), address(gameProxy));

        // Verify different chain IDs
        assertEq(game5.chainId(), 5);
        assertEq(game6.chainId(), 6);

        // Verify different root claims
        assertEq(game5.rootClaim().raw(), outputRootChain5);
        assertEq(game6.rootClaim().raw(), outputRootChain6);

        // Verify both delegate status to same SuperGame
        assertEq(uint256(game5.status()), uint256(gameProxy.status()));
        assertEq(uint256(game6.status()), uint256(gameProxy.status()));
    }

    /// @notice Tests that create reverts with invalid output root proof.
    function test_create_invalidOutputRootProof_reverts() public {
        // Create a corrupted proof (different stateRoot than what was used).
        Types.OutputRootProof memory corruptedProof = proofChain5;
        corruptedProof.stateRoot = bytes32(uint256(999));

        bytes memory extraData = _createExtendedExtraData(
            5000, address(gameProxy), 5, corruptedProof, headerRLPChain5
        );

        vm.expectRevert(DelegatedDisputeGame.InvalidOutputRootProof.selector);
        disputeGameFactory.create(DELEGATED_GAME_TYPE, Claim.wrap(outputRootChain5), extraData);
    }

    /// @notice Tests that create reverts with invalid header RLP.
    function test_create_invalidHeaderRLP_reverts() public {
        // Use invalid header RLP (doesn't hash to proof.latestBlockhash).
        bytes memory invalidHeaderRLP = hex"DEADBEEF";

        bytes memory extraData = _createExtendedExtraData(
            5000, address(gameProxy), 5, proofChain5, invalidHeaderRLP
        );

        vm.expectRevert(DelegatedDisputeGame.InvalidHeaderRLP.selector);
        disputeGameFactory.create(DELEGATED_GAME_TYPE, Claim.wrap(outputRootChain5), extraData);
    }

    /// @notice Tests that create reverts with mismatched block number.
    function test_create_blockNumberMismatch_reverts() public {
        // The proof was generated with block number 5000, but we claim 2000.
        uint256 wrongBlockNumber = 2000;

        bytes memory extraData = _createExtendedExtraData(
            wrongBlockNumber, address(gameProxy), 5, proofChain5, headerRLPChain5
        );

        vm.expectRevert(DelegatedDisputeGame.L2BlockNumberMismatch.selector);
        disputeGameFactory.create(DELEGATED_GAME_TYPE, Claim.wrap(outputRootChain5), extraData);
    }
}

/// @title DelegatedDisputeGame_TestInit
/// @notice Base test contract for DelegatedDisputeGame tests that creates a delegated game in setUp.
///         Uses real OutputRootProof hashes (no mocking).
contract DelegatedDisputeGame_TestInit is BaseSuperFaultDisputeGame_TestInit {
    /// @dev The game type for delegated games.
    GameType internal constant DELEGATED_GAME_TYPE = GameType.wrap(100);

    /// @dev The DelegatedDisputeGame implementation.
    DelegatedDisputeGame internal delegatedGameImpl;

    /// @dev A created DelegatedDisputeGame proxy.
    DelegatedDisputeGame internal delegatedGameProxy;

    /// @dev The chain ID for the delegated game.
    uint256 internal delegatedChainId;

    /// @dev The L2 block number for the delegated game (encoded in the proof).
    uint256 internal delegatedL2BlockNumber;

    /// @dev Stored proof data for chain 5.
    Types.OutputRootProof internal proofChain5;
    bytes internal headerRLPChain5;
    bytes32 internal outputRootChain5;

    /// @dev Stored proof data for chain 6.
    Types.OutputRootProof internal proofChain6;
    bytes internal headerRLPChain6;
    bytes32 internal outputRootChain6;

    /// @dev The root claim of the game.
    Claim internal ROOT_CLAIM;

    /// @dev The super root preimage of the game.
    Types.SuperRootProof SUPER_ROOT_PROOF;

    /// @dev The preimage of the absolute prestate claim.
    bytes internal absolutePrestateData;

    /// @dev The absolute prestate of the trace.
    Claim internal absolutePrestate;

    /// @dev A valid l2SequenceNumber that comes after the current anchor root block.
    uint256 validl2SequenceNumber;

    function setUp() public virtual override {
        absolutePrestateData = abi.encode(0);
        absolutePrestate = _changeClaimStatus(Claim.wrap(keccak256(absolutePrestateData)), VMStatuses.UNFINISHED);

        super.setUp();

        // Get the actual anchor roots.
        (, uint256 l2Seqno) = anchorStateRegistry.getAnchorRoot();
        validl2SequenceNumber = l2Seqno + 1;

        // Generate real OutputRootProofs for each chain.
        // Chain 5 has block number 5000, chain 6 has block number 6000.
        delegatedChainId = 5;
        delegatedL2BlockNumber = 5000;

        (proofChain5, outputRootChain5, headerRLPChain5) =
            _generateOutputRootProof(
                bytes32(uint256(5)),
                bytes32(uint256(delegatedL2BlockNumber)),
                abi.encodePacked(delegatedL2BlockNumber)
            );

        (proofChain6, outputRootChain6, headerRLPChain6) =
            _generateOutputRootProof(bytes32(uint256(6)), bytes32(uint256(6000)), abi.encodePacked(uint256(6000)));

        // Build SUPER_ROOT_PROOF with real OutputRootProof hashes.
        SUPER_ROOT_PROOF.version = bytes1(uint8(1));
        SUPER_ROOT_PROOF.timestamp = uint64(validl2SequenceNumber);
        SUPER_ROOT_PROOF.outputRoots.push(Types.OutputRootWithChainId({ chainId: 5, root: outputRootChain5 }));
        SUPER_ROOT_PROOF.outputRoots.push(Types.OutputRootWithChainId({ chainId: 6, root: outputRootChain6 }));
        ROOT_CLAIM = Claim.wrap(Hashing.hashSuperRootProof(SUPER_ROOT_PROOF));

        // Initialize the SuperGame with real output roots.
        init({ _rootClaim: ROOT_CLAIM, _absolutePrestate: absolutePrestate, _super: SUPER_ROOT_PROOF });

        // Deploy the DelegatedDisputeGame implementation with constructor args.
        delegatedGameImpl = new DelegatedDisputeGame(DELEGATED_GAME_TYPE, anchorStateRegistry);

        // Register the implementation with the factory.
        disputeGameFactory.setImplementation(DELEGATED_GAME_TYPE, delegatedGameImpl);

        // Set init bond to 0 for delegated games (no bonds).
        disputeGameFactory.setInitBond(DELEGATED_GAME_TYPE, 0);

        // Create a delegated game for chain 5 with the correct block number.
        delegatedGameProxy = _createDelegatedGameChain5(delegatedL2BlockNumber);
    }

    /// @notice Helper to change the VM status byte of a claim.
    function _changeClaimStatus(Claim _claim, VMStatus _status) internal pure returns (Claim out_) {
        assembly {
            out_ := or(and(not(shl(248, 0xFF)), _claim), shl(248, _status))
        }
    }

    /// @notice Helper to generate a mock RLP encoded header and output root proof.
    function _generateOutputRootProof(
        bytes32 _storageRoot,
        bytes32 _withdrawalRoot,
        bytes memory _l2BlockNumber
    )
        internal
        pure
        returns (Types.OutputRootProof memory proof_, bytes32 root_, bytes memory rlp_)
    {
        // L2 Block header (minimal mock with block number at index 8)
        bytes[] memory rawHeaderRLP = new bytes[](9);
        rawHeaderRLP[0] = hex"83FACADE";
        rawHeaderRLP[1] = hex"83FACADE";
        rawHeaderRLP[2] = hex"83FACADE";
        rawHeaderRLP[3] = hex"83FACADE";
        rawHeaderRLP[4] = hex"83FACADE";
        rawHeaderRLP[5] = hex"83FACADE";
        rawHeaderRLP[6] = hex"83FACADE";
        rawHeaderRLP[7] = hex"83FACADE";
        rawHeaderRLP[8] = RLPWriter.writeBytes(_l2BlockNumber);
        rlp_ = RLPWriter.writeList(rawHeaderRLP);

        // Output root proof
        proof_ = Types.OutputRootProof({
            version: 0,
            stateRoot: _storageRoot,
            messagePasserStorageRoot: _withdrawalRoot,
            latestBlockhash: keccak256(rlp_)
        });
        root_ = Hashing.hashOutputRootProof(proof_);
    }

    /// @notice Helper to create extended extraData for delegated games with block number proof.
    function _createExtendedExtraData(
        uint256 _l2BlockNumber,
        address _superGame,
        uint256 _chainId,
        Types.OutputRootProof memory _proof,
        bytes memory _headerRLP
    )
        internal
        pure
        returns (bytes memory extraData_)
    {
        extraData_ = abi.encodePacked(
            _l2BlockNumber,
            _superGame,
            _chainId,
            _proof.version,
            _proof.stateRoot,
            _proof.messagePasserStorageRoot,
            _proof.latestBlockhash,
            _headerRLP
        );
    }

    /// @notice Helper to create a delegated game for chain 5 (uses stored proof data).
    function _createDelegatedGameChain5(uint256 _l2BlockNumber)
        internal
        returns (DelegatedDisputeGame game_)
    {
        bytes memory extraData = _createExtendedExtraData(
            _l2BlockNumber,
            address(gameProxy),
            5,
            proofChain5,
            headerRLPChain5
        );

        game_ = DelegatedDisputeGame(
            payable(address(disputeGameFactory.create(DELEGATED_GAME_TYPE, Claim.wrap(outputRootChain5), extraData)))
        );
    }

    /// @notice Helper to create a delegated game for chain 6 (uses stored proof data).
    function _createDelegatedGameChain6(uint256 _l2BlockNumber)
        internal
        returns (DelegatedDisputeGame game_)
    {
        bytes memory extraData = _createExtendedExtraData(
            _l2BlockNumber,
            address(gameProxy),
            6,
            proofChain6,
            headerRLPChain6
        );

        game_ = DelegatedDisputeGame(
            payable(address(disputeGameFactory.create(DELEGATED_GAME_TYPE, Claim.wrap(outputRootChain6), extraData)))
        );
    }

    /// @notice Helper to resolve the SuperGame (uncontested, DEFENDER_WINS).
    function _resolveSuperGame() internal {
        vm.warp(block.timestamp + 3 days + 12 hours);
        gameProxy.resolveClaim(0, 0);
        gameProxy.resolve();
    }

    /// @notice Helper to set SuperGame status using vm.store.
    /// @param _status The status to set.
    function _setSuperGameStatus(GameStatus _status) internal {
        // Status is stored at slot 0, offset 16 (as seen in SuperFaultDisputeGame tests).
        uint256 slot = uint256(vm.load(address(gameProxy), bytes32(0)));
        uint256 offset = 16 << 3;
        uint256 mask = 0xFF << offset;
        slot = (slot & ~mask) | (uint256(_status) << offset);
        vm.store(address(gameProxy), bytes32(0), bytes32(slot));
    }

    /// @dev Counter for generating unique extraData.
    uint256 internal gameCounter;

    /// @notice Helper to create a delegated game with unique extraData.
    /// @dev Uses a counter to vary the storage root in the proof, making each game unique.
    /// @param _chainId The chain ID (5 or 6).
    /// @return game_ The created delegated game.
    function _createUniqueDelegatedGame(uint256 _chainId) internal returns (DelegatedDisputeGame game_) {
        gameCounter++;

        // Get the expected block number for this chain.
        uint256 blockNumber = _chainId == 5 ? delegatedL2BlockNumber : 6000;

        // Generate a unique proof by varying the storage root.
        (Types.OutputRootProof memory proof, bytes32 outputRoot, bytes memory headerRLP) =
            _generateOutputRootProof(
                bytes32(gameCounter), // Unique storage root
                bytes32(uint256(blockNumber)),
                abi.encodePacked(blockNumber)
            );

        // This game uses a different proof than what's in SuperGame, so it won't match rootClaimByChainId.
        // For tests that need proper validation, use the stored proof data.
        // For tests that just need a unique game, this works.

        bytes memory extraData = _createExtendedExtraData(
            blockNumber,
            address(gameProxy),
            _chainId,
            proof,
            headerRLP
        );

        // Create the game. Note: this will fail RootClaimMismatch since outputRoot doesn't match SuperGame.
        // We need to create games that match the SuperGame's rootClaimByChainId.
        game_ = DelegatedDisputeGame(
            payable(address(disputeGameFactory.create(DELEGATED_GAME_TYPE, Claim.wrap(outputRoot), extraData)))
        );
    }
}

/// @title DelegatedDisputeGame_AnchorRegistry_Test
/// @notice Tests for DelegatedDisputeGame's interaction with AnchorStateRegistry.
contract DelegatedDisputeGame_AnchorRegistry_Test is DelegatedDisputeGame_TestInit {
    /// @notice Tests that isGameRegistered returns true for a delegated game.
    function test_isGameRegistered_succeeds() public view {
        assertTrue(anchorStateRegistry.isGameRegistered(delegatedGameProxy));
    }

    /// @notice Tests that isGameRegistered returns false for an unregistered game.
    function test_isGameRegistered_unregisteredGame_fails() public {
        // Create a game directly (not through factory) - it won't be registered.
        DelegatedDisputeGame unregisteredGame = new DelegatedDisputeGame(DELEGATED_GAME_TYPE, anchorStateRegistry);
        assertFalse(anchorStateRegistry.isGameRegistered(unregisteredGame));
    }

    /// @notice Tests that isGameProper returns true for a properly created delegated game.
    /// @dev Note: isGameProper checks registration, blacklist, retirement - NOT respected status.
    ///      Use isGameRespected to check if game type was respected at creation.
    function test_isGameProper_returnsTrue() public view {
        // isGameProper returns true because the game is registered, not blacklisted, not retired.
        // This is separate from whether the game type was respected at creation.
        assertTrue(anchorStateRegistry.isGameProper(delegatedGameProxy));
    }

    /// @notice Tests that isGameProper returns true when game type is respected.
    function test_isGameProper_whenRespected_succeeds() public {
        // Set the delegated game type as the respected type.
        vm.prank(anchorStateRegistry.superchainConfig().guardian());
        anchorStateRegistry.setRespectedGameType(DELEGATED_GAME_TYPE);

        // Create a new delegated game for chain 6 (chain 5 already used in setUp).
        DelegatedDisputeGame newGame = _createDelegatedGameChain6(6000);

        assertTrue(anchorStateRegistry.isGameProper(newGame));
    }

    /// @notice Tests that isGameProper returns false when game is blacklisted.
    function test_isGameProper_blacklisted_fails() public {
        // Set the delegated game type as respected first.
        vm.prank(anchorStateRegistry.superchainConfig().guardian());
        anchorStateRegistry.setRespectedGameType(DELEGATED_GAME_TYPE);

        // Create a new game for chain 6 when respected (chain 5 already used in setUp).
        DelegatedDisputeGame newGame = _createDelegatedGameChain6(6000);

        // Verify it's proper before blacklisting.
        assertTrue(anchorStateRegistry.isGameProper(newGame));

        // Blacklist the game.
        vm.prank(anchorStateRegistry.superchainConfig().guardian());
        anchorStateRegistry.blacklistDisputeGame(newGame);

        // Now it should not be proper.
        assertFalse(anchorStateRegistry.isGameProper(newGame));
    }

    /// @notice Tests that isGameRespected returns the correct value.
    function test_isGameRespected_whenRespected_succeeds() public {
        // Set the delegated game type as the respected type.
        vm.prank(anchorStateRegistry.superchainConfig().guardian());
        anchorStateRegistry.setRespectedGameType(DELEGATED_GAME_TYPE);

        // Create a new delegated game for chain 6 (chain 5 already used in setUp).
        DelegatedDisputeGame newGame = _createDelegatedGameChain6(6000);

        assertTrue(anchorStateRegistry.isGameRespected(newGame));
    }

    /// @notice Tests that isGameRespected returns false when not respected.
    function test_isGameRespected_whenNotRespected_fails() public view {
        // The delegated game was created when its type was not respected.
        assertFalse(anchorStateRegistry.isGameRespected(delegatedGameProxy));
    }

    /// @notice Tests that isGameResolved returns correct value based on SuperGame status.
    function test_isGameResolved_delegatesToSuperGame() public {
        // Before resolution.
        assertFalse(anchorStateRegistry.isGameResolved(delegatedGameProxy));

        // Resolve the SuperGame.
        _resolveSuperGame();

        // After resolution.
        assertTrue(anchorStateRegistry.isGameResolved(delegatedGameProxy));
    }

    /// @notice Tests that isGameFinalized returns true after delay.
    function test_isGameFinalized_afterDelay_succeeds() public {
        // Resolve the SuperGame.
        _resolveSuperGame();

        // Not finalized yet (within delay).
        assertFalse(anchorStateRegistry.isGameFinalized(delegatedGameProxy));

        // Warp past the finality delay.
        uint256 finalityDelay = anchorStateRegistry.disputeGameFinalityDelaySeconds();
        vm.warp(block.timestamp + finalityDelay + 1);

        // Now finalized.
        assertTrue(anchorStateRegistry.isGameFinalized(delegatedGameProxy));
    }

    /// @notice Tests that isGameFinalized returns false before delay.
    function test_isGameFinalized_beforeDelay_fails() public {
        // Resolve the SuperGame.
        _resolveSuperGame();

        // Within the delay period.
        uint256 finalityDelay = anchorStateRegistry.disputeGameFinalityDelaySeconds();
        vm.warp(block.timestamp + finalityDelay - 1);

        // Not finalized yet.
        assertFalse(anchorStateRegistry.isGameFinalized(delegatedGameProxy));
    }
}

/// @title DelegatedDisputeGame_Resolution_Test
/// @notice Tests for DelegatedDisputeGame resolution flow.
contract DelegatedDisputeGame_Resolution_Test is DelegatedDisputeGame_TestInit {
    /// @dev Event emitted when the game is resolved.
    event Resolved(GameStatus indexed status);

    /// @notice Tests that status reflects DEFENDER_WINS after SuperGame resolves.
    function test_status_afterDefenderWins() public {
        // Before resolution.
        assertEq(uint256(delegatedGameProxy.status()), uint256(GameStatus.IN_PROGRESS));

        // Resolve the SuperGame (uncontested = DEFENDER_WINS).
        _resolveSuperGame();

        // After resolution.
        assertEq(uint256(delegatedGameProxy.status()), uint256(GameStatus.DEFENDER_WINS));
    }

    /// @notice Tests that status reflects CHALLENGER_WINS when SuperGame is contested.
    function test_status_afterChallengerWins() public {
        // Use vm.store to set the SuperGame status directly.
        _setSuperGameStatus(GameStatus.CHALLENGER_WINS);

        // DelegatedGame should delegate to SuperGame's status.
        assertEq(uint256(delegatedGameProxy.status()), uint256(GameStatus.CHALLENGER_WINS));
    }

    /// @notice Tests that resolvedAt matches SuperGame's resolvedAt.
    function test_resolvedAt_matchesSuperGame() public {
        // Before resolution.
        assertEq(delegatedGameProxy.resolvedAt().raw(), 0);
        assertEq(gameProxy.resolvedAt().raw(), 0);

        // Resolve the SuperGame.
        _resolveSuperGame();

        // After resolution - both should have same resolvedAt.
        assertEq(delegatedGameProxy.resolvedAt().raw(), gameProxy.resolvedAt().raw());
        assertTrue(delegatedGameProxy.resolvedAt().raw() > 0);
    }

    /// @notice Tests that resolve() emits event when SuperGame is resolved.
    function test_resolve_emitsEvent() public {
        // Resolve the SuperGame first.
        _resolveSuperGame();

        // Call resolve on the delegated game - should emit Resolved event.
        vm.expectEmit(true, true, true, true);
        emit Resolved(GameStatus.DEFENDER_WINS);
        delegatedGameProxy.resolve();
    }

    /// @notice Tests that resolve() reverts when SuperGame is not resolved.
    function test_resolve_whileInProgress_reverts() public {
        // SuperGame is still IN_PROGRESS.
        vm.expectRevert(DelegatedDisputeGame.GameNotInProgress.selector);
        delegatedGameProxy.resolve();
    }

    /// @notice Tests that resolve returns correct status for CHALLENGER_WINS.
    function test_resolve_challengerWins_succeeds() public {
        // Set SuperGame to CHALLENGER_WINS.
        _setSuperGameStatus(GameStatus.CHALLENGER_WINS);

        // Call resolve - should succeed and return CHALLENGER_WINS.
        GameStatus status = delegatedGameProxy.resolve();
        assertEq(uint256(status), uint256(GameStatus.CHALLENGER_WINS));
    }

    /// @notice Tests multiple delegated games share resolution from same SuperGame.
    function test_multipleGames_sharedResolution() public {
        // Create another delegated game for chain 6.
        DelegatedDisputeGame game6 = _createDelegatedGameChain6(6000);

        // Both should be IN_PROGRESS.
        assertEq(uint256(delegatedGameProxy.status()), uint256(GameStatus.IN_PROGRESS));
        assertEq(uint256(game6.status()), uint256(GameStatus.IN_PROGRESS));

        // Resolve the SuperGame.
        _resolveSuperGame();

        // Both should now be DEFENDER_WINS.
        assertEq(uint256(delegatedGameProxy.status()), uint256(GameStatus.DEFENDER_WINS));
        assertEq(uint256(game6.status()), uint256(GameStatus.DEFENDER_WINS));

        // Both should have same resolvedAt.
        assertEq(delegatedGameProxy.resolvedAt().raw(), game6.resolvedAt().raw());
    }
}

/// @title DelegatedDisputeGame_EdgeCases_Test
/// @notice Tests for DelegatedDisputeGame edge cases.
contract DelegatedDisputeGame_EdgeCases_Test is DelegatedDisputeGame_TestInit {
    /// @notice Tests that a delegated game can be created after SuperGame is resolved.
    function test_create_afterSuperGameResolved_succeeds() public {
        // Resolve the SuperGame first.
        _resolveSuperGame();

        // Create a new delegated game for chain 6 (chain 5 already used in setUp).
        DelegatedDisputeGame newGame = _createDelegatedGameChain6(6000);

        // Verify the game was created and status is DEFENDER_WINS (delegates to resolved SuperGame).
        assertTrue(address(newGame) != address(0));
        assertEq(uint256(newGame.status()), uint256(GameStatus.DEFENDER_WINS));
    }

    /// @notice Tests that initialize reverts if called twice.
    function test_initialize_alreadyInitialized_reverts() public {
        // The game was already initialized during creation.
        vm.expectRevert(DelegatedDisputeGame.AlreadyInitialized.selector);
        delegatedGameProxy.initialize();
    }

    /// @notice Tests that creation fails with wrong chain ID (root claim mismatch).
    function test_create_mismatchedChainId_reverts() public {
        // Get root claim for chain 5 but try to use chain 6's extraData structure.
        // This creates extraData pointing to chain 6 but using chain 5's proof.
        bytes memory extraData = _createExtendedExtraData(
            delegatedL2BlockNumber,
            address(gameProxy),
            6, // Wrong chainId - should be 5 to match proofChain5
            proofChain5,
            headerRLPChain5
        );

        // Should revert because rootClaim (for chain 5's output root) doesn't match what SuperGame has for chain 6.
        vm.expectRevert(DelegatedDisputeGame.RootClaimMismatch.selector);
        disputeGameFactory.create(DELEGATED_GAME_TYPE, Claim.wrap(outputRootChain5), extraData);
    }

    /// @notice Tests that anchor state registry mismatch is caught.
    /// @dev Uses vm.mockCall to test this error path (acceptable per testing guidelines).
    function test_create_anchorRegistryMismatch_reverts() public {
        // Mock the SuperGame's anchorStateRegistry() to return a different address.
        address fakeRegistry = address(0xDEAD);
        vm.mockCall(
            address(gameProxy),
            abi.encodeWithSelector(ISuperFaultDisputeGame.anchorStateRegistry.selector),
            abi.encode(fakeRegistry)
        );

        // Try to create a delegated game for chain 6 (chain 5 already used in setUp).
        bytes memory extraData = _createExtendedExtraData(
            6000,
            address(gameProxy),
            6,
            proofChain6,
            headerRLPChain6
        );

        vm.expectRevert(DelegatedDisputeGame.AnchorStateRegistryMismatch.selector);
        disputeGameFactory.create(DELEGATED_GAME_TYPE, Claim.wrap(outputRootChain6), extraData);

        // Clear the mock.
        vm.clearMockedCalls();
    }

    /// @notice Tests that SuperGame address zero is rejected.
    function test_create_invalidSuperGame_reverts() public {
        // Create extraData with address(0) as superGame.
        bytes memory extraData = _createExtendedExtraData(
            6000,
            address(0), // Invalid zero address
            6,
            proofChain6,
            headerRLPChain6
        );

        vm.expectRevert(DelegatedDisputeGame.InvalidSuperGame.selector);
        disputeGameFactory.create(DELEGATED_GAME_TYPE, Claim.wrap(outputRootChain6), extraData);
    }

    /// @notice Tests that resolve can be called multiple times after resolution.
    function test_resolve_multipleCalls_succeeds() public {
        // Resolve the SuperGame.
        _resolveSuperGame();

        // First call to resolve.
        GameStatus status1 = delegatedGameProxy.resolve();
        assertEq(uint256(status1), uint256(GameStatus.DEFENDER_WINS));

        // Second call to resolve (should also succeed).
        GameStatus status2 = delegatedGameProxy.resolve();
        assertEq(uint256(status2), uint256(GameStatus.DEFENDER_WINS));
    }
}

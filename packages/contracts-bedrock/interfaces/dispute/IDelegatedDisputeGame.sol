// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

import { IDisputeGame } from "interfaces/dispute/IDisputeGame.sol";
import { ISuperFaultDisputeGame } from "interfaces/dispute/ISuperFaultDisputeGame.sol";
import { IAnchorStateRegistry } from "interfaces/dispute/IAnchorStateRegistry.sol";

/// @title IDelegatedDisputeGame
/// @notice Minimal interface for DelegatedDisputeGame, used by AnchorStateRegistry
///         to check SuperGame validity.
interface IDelegatedDisputeGame is IDisputeGame {
    /// @notice Returns the super game this delegated game is linked to.
    /// @return superGame_ The super fault dispute game.
    function superGame() external view returns (ISuperFaultDisputeGame superGame_);

    /// @notice Returns the chain ID this delegated game is for.
    /// @return chainId_ The chain ID.
    function chainId() external view returns (uint256 chainId_);

    /// @notice Returns the anchor state registry.
    /// @return registry_ The anchor state registry.
    function anchorStateRegistry() external view returns (IAnchorStateRegistry registry_);
}

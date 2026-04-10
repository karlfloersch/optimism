use crate::supervisor::InteropTxValidatorError;
use reth_transaction_pool::error::PoolTransactionError;
use std::any::Any;

/// Wrapper for [`InteropTxValidatorError`] to implement [`PoolTransactionError`] for it.
#[derive(thiserror::Error, Debug)]
pub enum InvalidCrossTx {
    /// Errors produced by supervisor validation
    #[error(transparent)]
    ValidationError(#[from] InteropTxValidatorError),
    /// Error cause by cross chain tx during not active interop hardfork
    #[error("cross chain tx is invalid before interop")]
    CrossChainTxPreInterop,
    /// Sender is not in the interop allowed senders list
    #[error("interop tx sender not allowed: {0}")]
    SenderNotAllowed(alloy_primitives::Address),
}

impl PoolTransactionError for InvalidCrossTx {
    fn is_bad_transaction(&self) -> bool {
        match self {
            Self::ValidationError(_) => false,
            Self::CrossChainTxPreInterop => true,
            Self::SenderNotAllowed(_) => true,
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

package apis

import (
	"context"

	"github.com/ethereum/go-ethereum/common"

	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/types"
)

// InteropFilterAPI is the RPC API interface for the interop filter service.
// It implements a subset of the supervisor API for transaction filtering.
type InteropFilterAPI interface {
	InteropFilterAdminAPI
	InteropFilterQueryAPI
}

// InteropFilterAdminAPI provides admin methods for the interop filter.
type InteropFilterAdminAPI interface {
	GetFailsafeEnabled(ctx context.Context) (bool, error)
}

// InteropFilterQueryAPI provides query methods for the interop filter.
type InteropFilterQueryAPI interface {
	CheckAccessList(ctx context.Context, inboxEntries []common.Hash,
		minSafety types.SafetyLevel, executingDescriptor types.ExecutingDescriptor) error
}

package sources

import (
	"context"
	"fmt"

	"github.com/ethereum/go-ethereum/common"

	"github.com/ethereum-optimism/optimism/op-service/apis"
	"github.com/ethereum-optimism/optimism/op-service/client"
	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/types"
)

// InteropFilterClient is an RPC client for the interop filter service.
type InteropFilterClient struct {
	client client.RPC
}

var _ apis.InteropFilterAPI = (*InteropFilterClient)(nil)

// NewInteropFilterClient creates a new InteropFilterClient.
func NewInteropFilterClient(client client.RPC) *InteropFilterClient {
	return &InteropFilterClient{
		client: client,
	}
}

// GetFailsafeEnabled returns whether failsafe is enabled.
func (cl *InteropFilterClient) GetFailsafeEnabled(ctx context.Context) (bool, error) {
	var enabled bool
	err := cl.client.CallContext(ctx, &enabled, "admin_getFailsafeEnabled")
	if err != nil {
		return false, fmt.Errorf("failed to get failsafe mode for interop filter: %w", err)
	}
	return enabled, nil
}

// CheckAccessList validates interop executing messages.
func (cl *InteropFilterClient) CheckAccessList(ctx context.Context, inboxEntries []common.Hash,
	minSafety types.SafetyLevel, executingDescriptor types.ExecutingDescriptor) error {
	return cl.client.CallContext(ctx, nil, "supervisor_checkAccessList", inboxEntries, minSafety, executingDescriptor)
}

// Close closes the underlying RPC client.
func (cl *InteropFilterClient) Close() {
	cl.client.Close()
}

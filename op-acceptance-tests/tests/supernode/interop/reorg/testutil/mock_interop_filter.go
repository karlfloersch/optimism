package testutil

import (
	"github.com/ethereum-optimism/optimism/op-devstack/devtest"
	oprpc "github.com/ethereum-optimism/optimism/op-service/rpc"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types/interoptypes"
	"github.com/ethereum/go-ethereum/rpc"
)

// supervisorAPI implements the supervisor_ namespace.
// The method signatures must match what geth's InteropClient expects.
type supervisorAPI struct{}

func (s *supervisorAPI) CheckAccessList(
	inboxEntries []common.Hash,
	minSafety interoptypes.SafetyLevel,
	executingDescriptor interoptypes.ExecutingDescriptor,
) error {
	return nil // always approve
}

// adminAPI implements the admin_ namespace.
type adminAPI struct{}

func (a *adminAPI) GetFailsafeEnabled() bool {
	return false
}

// StartMockInteropFilter starts a mock interop filter RPC server and returns
// its HTTP endpoint URL. The server always approves checkAccessList requests,
// which is sufficient to activate geth's ingress filter registration
// (len(pool.ingressFilters) > 0) and thus the interop tx eviction on reorg.
func StartMockInteropFilter(t devtest.T) string {
	server := oprpc.NewServer(
		"127.0.0.1",
		0, // auto-assign port
		"mock-interop-filter",
	)

	server.AddAPI(rpc.API{
		Namespace: "supervisor",
		Service:   &supervisorAPI{},
	})
	server.AddAPI(rpc.API{
		Namespace: "admin",
		Service:   &adminAPI{},
	})

	t.Require().NoError(server.Start())
	t.Cleanup(func() { _ = server.Stop() })

	return "http://" + server.Endpoint()
}

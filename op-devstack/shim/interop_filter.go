package shim

import (
	"github.com/ethereum-optimism/optimism/op-devstack/stack"
	"github.com/ethereum-optimism/optimism/op-service/apis"
	"github.com/ethereum-optimism/optimism/op-service/client"
	"github.com/ethereum-optimism/optimism/op-service/sources"
)

type InteropFilterConfig struct {
	CommonConfig
	ID     stack.InteropFilterID
	Client client.RPC
}

type rpcInteropFilter struct {
	commonImpl
	id stack.InteropFilterID

	client client.RPC
	api    apis.InteropFilterAPI
}

var _ stack.InteropFilter = (*rpcInteropFilter)(nil)

func NewInteropFilter(cfg InteropFilterConfig) stack.InteropFilter {
	cfg.T = cfg.T.WithCtx(stack.ContextWithID(cfg.T.Ctx(), cfg.ID))
	return &rpcInteropFilter{
		commonImpl: newCommon(cfg.CommonConfig),
		id:         cfg.ID,
		client:     cfg.Client,
		api:        sources.NewInteropFilterClient(cfg.Client),
	}
}

func (r *rpcInteropFilter) ID() stack.InteropFilterID {
	return r.id
}

func (r *rpcInteropFilter) AdminAPI() apis.InteropFilterAdminAPI {
	return r.api
}

func (r *rpcInteropFilter) QueryAPI() apis.InteropFilterQueryAPI {
	return r.api
}

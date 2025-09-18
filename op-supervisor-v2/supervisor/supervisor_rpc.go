package supervisor

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/ethereum-optimism/optimism/op-service/eth"
	supervisorTypes "github.com/ethereum-optimism/optimism/op-supervisor/supervisor/types"
	"github.com/ethereum/go-ethereum/common"
)

// SupervisorRPCAPI provides JSON-RPC methods for the supervisor
type SupervisorRPCAPI struct {
	supervisor *Supervisor
}

type executingDescriptor struct {
	ChainID   string      `json:"chainID"`
	Timestamp interface{} `json:"timestamp"`
	Timeout   interface{} `json:"timeout"`
}

// SupervisorCheckAccessList handles the supervisor_checkAccessList RPC method
// This method is called by geth for interop mempool filtering
func (api *SupervisorRPCAPI) CheckAccessList(ctx context.Context, inboxEntries []string, minSafety string, executingDescriptor executingDescriptor) (bool, error) {
	api.supervisor.log.Info("Received RPC access list check request", "function", "SupervisorCheckAccessList", "entries", len(inboxEntries), "min_safety", minSafety)

	// Convert entries
	hashes := make([]common.Hash, len(inboxEntries))
	for i, e := range inboxEntries {
		hashes[i] = common.HexToHash(e)
	}

	// Parse minSafety
	var level supervisorTypes.SafetyLevel
	switch minSafety {
	case "LocalUnsafe":
		level = supervisorTypes.LocalUnsafe
	default:
		level = supervisorTypes.LocalUnsafe
		api.supervisor.log.Warn("always using LocalUnsafe for SV2 Demo Purposes")
	}

	// Build executing descriptor
	var exec supervisorTypes.ExecutingDescriptor
	if executingDescriptor.ChainID != "" {
		chainIDHash := common.HexToHash(executingDescriptor.ChainID)
		exec.ChainID = eth.ChainIDFromBytes32(chainIDHash)
	}
	// Parse timestamp (number or string)
	switch v := executingDescriptor.Timestamp.(type) {
	case float64:
		exec.Timestamp = uint64(v)
	case json.Number:
		if n, err := v.Int64(); err == nil {
			exec.Timestamp = uint64(n)
		} else {
			return false, fmt.Errorf("bad timestamp: %v", err)
		}
	case string:
		var n uint64
		var err error
		// Handle both hex (0x...) and decimal string formats
		if strings.HasPrefix(v, "0x") || strings.HasPrefix(v, "0X") {
			// Parse as hex
			n, err = strconv.ParseUint(v, 0, 64)
		} else {
			// Parse as decimal
			_, err = fmtSscanf(v, &n)
		}
		if err != nil {
			return false, fmt.Errorf("bad timestamp string: %v", err)
		}
		exec.Timestamp = n
	case nil:
		exec.Timestamp = 0
	default:
		return false, fmt.Errorf("unsupported timestamp type")
	}

	// Parse timeout (number or string)
	switch v := executingDescriptor.Timeout.(type) {
	case float64:
		exec.Timeout = uint64(v)
	case json.Number:
		if n, err := v.Int64(); err == nil {
			exec.Timeout = uint64(n)
		} else {
			return false, fmt.Errorf("bad timeout: %v", err)
		}
	case string:
		var n uint64
		var err error
		// Handle both hex (0x...) and decimal string formats
		if strings.HasPrefix(v, "0x") || strings.HasPrefix(v, "0X") {
			// Parse as hex
			n, err = strconv.ParseUint(v, 0, 64)
		} else {
			// Parse as decimal
			_, err = fmtSscanf(v, &n)
		}
		if err != nil {
			return false, fmt.Errorf("bad timeout string: %v", err)
		}
		exec.Timeout = n
	case nil:
		exec.Timeout = 0
	default:
		return false, fmt.Errorf("unsupported timeout type")
	}

	if err := api.supervisor.CheckAccessList(ctx, hashes, level, exec); err != nil {
		return false, err
	}
	return true, nil
}

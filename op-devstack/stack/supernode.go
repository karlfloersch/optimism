package stack

import (
	"fmt"
	"sort"

	"github.com/ethereum-optimism/optimism/op-service/apis"
	"github.com/ethereum-optimism/optimism/op-service/eth"
)

// SupernodeID is kept as a semantic alias for ComponentID.
// Supernode IDs are key-only IDs with KindSupernode.
type SupernodeID = ComponentID

func NewSupernodeID(key string, chains ...eth.ChainID) SupernodeID {
	var suffix string
	for _, chain := range chains {
		suffix += chain.String()
	}
	return NewComponentIDKeyOnly(KindSupernode, fmt.Sprintf("%s-%s", key, suffix))
}

func SortSupernodeIDs(ids []SupernodeID) []SupernodeID {
	out := append([]SupernodeID(nil), ids...)
	sort.Slice(out, func(i, j int) bool {
		return out[i].Less(out[j])
	})
	return out
}

func SortSupernodes(elems []Supernode) []Supernode {
	out := append([]Supernode(nil), elems...)
	sort.Slice(out, func(i, j int) bool {
		return out[i].ID().Less(out[j].ID())
	})
	return out
}

type Supernode interface {
	Common
	ID() ComponentID
	QueryAPI() apis.SupernodeQueryAPI
}

type InteropDebugSnapshot struct {
	Timestamp   uint64                      `json:"timestamp"`
	L1Inclusion eth.BlockID                 `json:"l1_inclusion"`
	L1Heads     map[eth.ChainID]eth.BlockID `json:"l1_heads"`
	L2Heads     map[eth.ChainID]eth.BlockID `json:"l2_heads"`
}

type InteropDebugState struct {
	Accepted *InteropDebugSnapshot `json:"accepted,omitempty"`
	Frontier *InteropDebugSnapshot `json:"frontier,omitempty"`
	NextTS   uint64                `json:"next_ts"`
}

// InteropTestControl provides integration test control methods for the interop activity.
// This interface is for integration test control only.
type InteropTestControl interface {
	// PauseInteropActivity pauses the interop activity at the given timestamp.
	// When the interop activity attempts to process this timestamp, it returns early.
	// This function is for integration test control only.
	PauseInteropActivity(ts uint64)

	// ResumeInteropActivity clears any pause on the interop activity, allowing normal processing.
	// This function is for integration test control only.
	ResumeInteropActivity()

	// PauseInteropAfterNextResetActivity arms a one-shot pause that triggers
	// after the next reset which causes interop to retry the given timestamp.
	// This function is for integration test control only.
	PauseInteropAfterNextResetActivity(ts uint64)

	// InteropDebugState returns a test-only snapshot of the current accepted and
	// next frontier interop state, if available.
	InteropDebugState() (*InteropDebugState, error)
}

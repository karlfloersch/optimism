package interop

import (
	"context"

	"github.com/ethereum-optimism/optimism/op-service/eth"
)

type byNumberConsistencyChecker struct {
	l1 l1ByNumberSource
}

func newByNumberConsistencyChecker(l1 l1ByNumberSource) *byNumberConsistencyChecker {
	if l1 == nil {
		return nil
	}
	return &byNumberConsistencyChecker{l1: l1}
}

func (c *byNumberConsistencyChecker) SameL1Chain(ctx context.Context, heads []eth.BlockID) (bool, error) {
	for _, head := range heads {
		if head == (eth.BlockID{}) {
			continue
		}
		canonical, err := c.l1.L1BlockRefByNumber(ctx, head.Number)
		if err != nil {
			return false, err
		}
		if canonical.Hash != head.Hash {
			return false, nil
		}
	}
	return true, nil
}

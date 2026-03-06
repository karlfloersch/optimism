package interop

import (
	"context"
	"reflect"

	"github.com/ethereum-optimism/optimism/op-service/eth"
)

type L1BlockRefSource interface {
	L1BlockRefByNumber(ctx context.Context, num uint64) (eth.L1BlockRef, error)
}

type ConsistencyChecker interface {
	AcceptedResultConsistent(ctx context.Context, stored VerifiedResult, current VerifiedResult) (bool, error)
	FrontierConsistent(ctx context.Context, accepted VerifiedResult, frontier VerifiedResult) (bool, error)
}

type ByNumberChecker struct {
	l1 L1BlockRefSource
}

func NewByNumberChecker(l1 L1BlockRefSource) *ByNumberChecker {
	return &ByNumberChecker{l1: l1}
}

func (c *ByNumberChecker) AcceptedResultConsistent(ctx context.Context, stored VerifiedResult, current VerifiedResult) (bool, error) {
	ok, err := c.snapshotCanonical(ctx, current)
	if err != nil || !ok {
		return ok, err
	}
	return verifiedResultsEqual(stored, current), nil
}

func (c *ByNumberChecker) FrontierConsistent(ctx context.Context, accepted VerifiedResult, frontier VerifiedResult) (bool, error) {
	ok, err := c.snapshotCanonical(ctx, frontier)
	if err != nil || !ok {
		return ok, err
	}
	if accepted.Timestamp == 0 && len(accepted.L2Heads) == 0 {
		return true, nil
	}
	if frontier.Timestamp != accepted.Timestamp+1 {
		return false, nil
	}
	for chainID, frontierHead := range frontier.L2Heads {
		acceptedHead, ok := accepted.L2Heads[chainID]
		if !ok {
			return false, nil
		}
		if frontierHead.Number < acceptedHead.Number {
			return false, nil
		}
		if frontierHead.Number == acceptedHead.Number && frontierHead.Hash != acceptedHead.Hash {
			return false, nil
		}
	}
	return true, nil
}

func (c *ByNumberChecker) snapshotCanonical(ctx context.Context, snapshot VerifiedResult) (bool, error) {
	if maxL1Head(snapshot.L1Heads) != snapshot.L1Inclusion {
		return false, nil
	}
	if c.l1 == nil {
		return true, nil
	}
	for _, head := range snapshot.L1Heads {
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

func verifiedResultsEqual(a, b VerifiedResult) bool {
	return a.Timestamp == b.Timestamp &&
		a.L1Inclusion == b.L1Inclusion &&
		reflect.DeepEqual(a.L1Heads, b.L1Heads) &&
		reflect.DeepEqual(a.L2Heads, b.L2Heads)
}

func maxL1Head(heads map[eth.ChainID]eth.BlockID) eth.BlockID {
	var maxHead eth.BlockID
	first := true
	for _, head := range heads {
		if first || head.Number >= maxHead.Number {
			maxHead = head
			first = false
		}
	}
	return maxHead
}

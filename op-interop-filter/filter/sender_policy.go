package filter

import (
	"fmt"
	"strings"

	"github.com/ethereum/go-ethereum/common"
)

// SenderPolicy controls which senders may submit interop transactions.
type SenderPolicy struct {
	allowAny bool
	allowed  map[common.Address]struct{}
}

func ParseSenderPolicy(raw string) (*SenderPolicy, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, fmt.Errorf("allowed-senders must not be empty")
	}
	if raw == "*" {
		return &SenderPolicy{allowAny: true}, nil
	}

	parts := strings.Split(raw, ",")
	allowed := make(map[common.Address]struct{}, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			return nil, fmt.Errorf("allowed-senders contains an empty entry")
		}
		if part == "*" {
			return nil, fmt.Errorf("allowed-senders wildcard '*' must be used by itself")
		}
		if !common.IsHexAddress(part) {
			return nil, fmt.Errorf("invalid sender address %q", part)
		}
		allowed[common.HexToAddress(part)] = struct{}{}
	}
	return &SenderPolicy{allowed: allowed}, nil
}

func AllowAnySenderPolicy() *SenderPolicy {
	return &SenderPolicy{allowAny: true}
}

func (p *SenderPolicy) Allows(sender common.Address) bool {
	if p == nil {
		return false
	}
	if p.allowAny {
		return true
	}
	_, ok := p.allowed[sender]
	return ok
}

func (p *SenderPolicy) AllowsAny() bool {
	return p != nil && p.allowAny
}

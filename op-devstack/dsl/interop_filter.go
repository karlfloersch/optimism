package dsl

import (
	"github.com/ethereum-optimism/optimism/op-devstack/stack"
)

type InteropFilter struct {
	commonImpl
	inner stack.InteropFilter
}

// NewInteropFilter creates a new InteropFilter DSL wrapper
func NewInteropFilter(inner stack.InteropFilter) *InteropFilter {
	return &InteropFilter{
		commonImpl: commonFromT(inner.T()),
		inner:      inner,
	}
}

// Escape returns the underlying stack.InteropFilter
func (f *InteropFilter) Escape() stack.InteropFilter {
	return f.inner
}

// GetFailsafeEnabled returns whether failsafe is enabled
func (f *InteropFilter) GetFailsafeEnabled() bool {
	enabled, err := f.inner.AdminAPI().GetFailsafeEnabled(f.ctx)
	f.require.NoError(err, "failed to get failsafe enabled")
	return enabled
}

// CheckAccessList validates interop executing messages
func (f *InteropFilter) CheckAccessList(inboxEntries [][]byte) error {
	// TODO: implement when needed for tests
	return nil
}

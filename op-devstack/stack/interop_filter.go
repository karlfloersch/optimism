package stack

import (
	"log/slog"

	"github.com/ethereum-optimism/optimism/op-service/apis"
)

// InteropFilterID identifies an InteropFilter by name, is type-safe, and can be value-copied and used as map key.
type InteropFilterID genericID

var _ GenericID = (*InteropFilterID)(nil)

const InteropFilterKind Kind = "InteropFilter"

func (id InteropFilterID) String() string {
	return genericID(id).string(InteropFilterKind)
}

func (id InteropFilterID) Kind() Kind {
	return InteropFilterKind
}

func (id InteropFilterID) LogValue() slog.Value {
	return slog.StringValue(id.String())
}

func (id InteropFilterID) MarshalText() ([]byte, error) {
	return genericID(id).marshalText(InteropFilterKind)
}

func (id *InteropFilterID) UnmarshalText(data []byte) error {
	return (*genericID)(id).unmarshalText(InteropFilterKind, data)
}

func SortInteropFilterIDs(ids []InteropFilterID) []InteropFilterID {
	return copyAndSortCmp(ids)
}

func SortInteropFilters(elems []InteropFilter) []InteropFilter {
	return copyAndSort(elems, lessElemOrdered[InteropFilterID, InteropFilter])
}

var _ InteropFilterMatcher = InteropFilterID("")

func (id InteropFilterID) Match(elems []InteropFilter) []InteropFilter {
	return findByID(id, elems)
}

// InteropFilter is a lightweight service that validates interop executing messages.
// It provides a subset of supervisor functionality focused on transaction filtering.
type InteropFilter interface {
	Common
	ID() InteropFilterID

	AdminAPI() apis.InteropFilterAdminAPI
	QueryAPI() apis.InteropFilterQueryAPI
}

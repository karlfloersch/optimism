package interop

import (
	"encoding/json"

	"github.com/ethereum-optimism/optimism/op-service/eth"
)

func (i *Interop) denyEntryMetadata(chainID eth.ChainID, result Result) ([]byte, error) {
	_ = chainID
	return json.Marshal(result)
}

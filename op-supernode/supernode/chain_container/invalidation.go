package chain_container

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	bolt "go.etcd.io/bbolt"
)

const (
	denyListDBName = "denylist"
)

// denyListBucketName is the name of the bbolt bucket used to store denied block hashes.
var denyListBucketName = []byte("denied_blocks")

// DenyList provides persistence for invalid block payload hashes using bbolt.
// Blocks are keyed by block height, with each height potentially having multiple denied hashes.
type DenyList struct {
	db *bolt.DB
	mu sync.RWMutex
}

type DenyEntry struct {
	PayloadHash common.Hash     `json:"payloadHash"`
	Result      json.RawMessage `json:"result,omitempty"`
}

// OpenDenyList opens or creates a DenyList at the given data directory.
func OpenDenyList(dataDir string) (*DenyList, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create denylist directory %s: %w", dataDir, err)
	}
	dbPath := filepath.Join(dataDir, denyListDBName+".db")
	db, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to open denylist bbolt at %s: %w", dbPath, err)
	}

	// Ensure the bucket exists
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(denyListBucketName)
		return err
	})
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create denylist bucket: %w", err)
	}

	return &DenyList{db: db}, nil
}

// heightToKey converts a block height to a big-endian byte key.
// Using big-endian ensures lexicographic ordering matches numeric ordering.
func heightToKey(height uint64) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, height)
	return key
}

// Add adds a payload hash to the deny list at the given block height.
// Multiple hashes can be denied at the same height.
func (d *DenyList) Add(height uint64, payloadHash common.Hash) error {
	return d.AddEntry(height, DenyEntry{PayloadHash: payloadHash})
}

// AddEntry adds a structured deny entry at the given block height.
func (d *DenyList) AddEntry(height uint64, entry DenyEntry) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	key := heightToKey(height)

	return d.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(denyListBucketName)
		entries, err := decodeEntries(b.Get(key))
		if err != nil {
			return err
		}
		entries = append([]DenyEntry{entry}, entries...)
		encoded, err := json.Marshal(entries)
		if err != nil {
			return err
		}
		return b.Put(key, encoded)
	})
}

// Contains checks if a payload hash is denied at the given block height.
func (d *DenyList) Contains(height uint64, payloadHash common.Hash) (bool, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	key := heightToKey(height)
	var found bool

	err := d.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(denyListBucketName)
		entries, err := decodeEntries(b.Get(key))
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if entry.PayloadHash == payloadHash {
				found = true
				return nil
			}
		}
		return nil
	})

	return found, err
}

// GetDeniedHashes returns all denied payload hashes at the given block height.
func (d *DenyList) GetDeniedHashes(height uint64) ([]common.Hash, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	key := heightToKey(height)
	var hashes []common.Hash

	err := d.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(denyListBucketName)
		entries, err := decodeEntries(b.Get(key))
		if err != nil {
			return err
		}
		if len(entries) == 0 {
			return nil
		}
		seen := make(map[common.Hash]struct{})
		for _, entry := range entries {
			if _, ok := seen[entry.PayloadHash]; ok {
				continue
			}
			seen[entry.PayloadHash] = struct{}{}
			hashes = append(hashes, entry.PayloadHash)
		}
		return nil
	})

	return hashes, err
}

// GetEntries returns all deny entry versions for a payload at the given block height.
// Entries are returned newest-first.
func (d *DenyList) GetEntries(height uint64, payloadHash common.Hash) ([]DenyEntry, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	key := heightToKey(height)
	var entries []DenyEntry

	err := d.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(denyListBucketName)
		decoded, err := decodeEntries(b.Get(key))
		if err != nil {
			return err
		}
		for _, entry := range decoded {
			if entry.PayloadHash == payloadHash {
				entries = append(entries, entry)
			}
		}
		return nil
	})

	return entries, err
}

// Close closes the database.
func (d *DenyList) Close() error {
	return d.db.Close()
}

// InvalidateBlock adds a block to the deny list and triggers a rewind if the chain
// currently uses that block at the specified height.
// Returns true if a rewind was triggered, false otherwise.
// Note: Genesis block (height=0) cannot be invalidated as there is no prior block to rewind to.
func (c *simpleChainContainer) InvalidateBlock(ctx context.Context, height uint64, payloadHash common.Hash, resultMetadata []byte) (bool, error) {
	if c.denyList == nil {
		return false, fmt.Errorf("deny list not initialized")
	}

	// Cannot invalidate genesis block - there is no prior block to rewind to
	if height == 0 {
		return false, fmt.Errorf("cannot invalidate genesis block (height=0)")
	}

	// Add to deny list first
	if err := c.denyList.AddEntry(height, DenyEntry{PayloadHash: payloadHash, Result: resultMetadata}); err != nil {
		return false, fmt.Errorf("failed to add block to deny list: %w", err)
	}

	c.log.Info("added block to deny list",
		"height", height,
		"payloadHash", payloadHash,
	)

	// Check if the current chain uses this block at this height
	if c.engine == nil {
		c.log.Warn("engine not initialized, cannot check current block")
		return false, nil
	}

	currentBlock, err := c.engine.L2BlockRefByNumber(ctx, height)
	if err != nil {
		c.log.Warn("failed to get current block at height", "height", height, "err", err)
		return false, nil
	}

	// Compare the current block hash with the invalidated hash
	if currentBlock.Hash != payloadHash {
		c.log.Info("current block differs from invalidated block, no rewind needed",
			"height", height,
			"currentHash", currentBlock.Hash,
			"invalidatedHash", payloadHash,
		)
		return false, nil
	}

	c.log.Warn("current block matches invalidated block, initiating rewind",
		"height", height,
		"hash", payloadHash,
	)

	invalidatedBlock := currentBlock.BlockRef()

	// Rewind to the prior block's timestamp
	priorTimestamp := c.blockNumberToTimestamp(height - 1)
	if err := c.RewindEngine(ctx, priorTimestamp, invalidatedBlock); err != nil {
		return false, fmt.Errorf("failed to rewind engine: %w", err)
	}

	c.log.Info("rewind completed after block invalidation",
		"invalidatedHeight", height,
		"rewindToTimestamp", priorTimestamp,
	)

	return true, nil
}

func decodeEntries(raw []byte) ([]DenyEntry, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var entries []DenyEntry
	if err := json.Unmarshal(raw, &entries); err == nil {
		return entries, nil
	}
	// Legacy support: raw concatenated hashes.
	if len(raw)%common.HashLength != 0 {
		return nil, fmt.Errorf("invalid deny list payload")
	}
	entries = make([]DenyEntry, 0, len(raw)/common.HashLength)
	for i := 0; i+common.HashLength <= len(raw); i += common.HashLength {
		entries = append(entries, DenyEntry{PayloadHash: common.BytesToHash(raw[i : i+common.HashLength])})
	}
	return entries, nil
}

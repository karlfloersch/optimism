package store

import (
	"encoding/json"
	"fmt"
	"path/filepath"

	interopengine "github.com/ethereum-optimism/optimism/op-supernode/supernode/activity/interop/engine"
	bolt "go.etcd.io/bbolt"
)

const (
	dbName     = "InteropState.db"
	bucket     = "state"
	currentKey = "current"
)

type Store struct {
	db *bolt.DB
}

func Open(dir string) (*Store, error) {
	dbPath := filepath.Join(dir, dbName)
	db, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		return nil, fmt.Errorf("open interop state db: %w", err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bucket))
		return err
	}); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("create interop state bucket: %w", err)
	}
	return &Store{db: db}, nil
}

func (s *Store) Load() (interopengine.InteropState, error) {
	var out interopengine.InteropState
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		raw := b.Get([]byte(currentKey))
		if raw == nil {
			out = interopengine.InteropState{}
			return nil
		}
		var encoded encodedState
		if err := json.Unmarshal(raw, &encoded); err != nil {
			return fmt.Errorf("decode interop state: %w", err)
		}
		decoded, err := encoded.decode()
		if err != nil {
			return err
		}
		out = decoded
		return nil
	})
	if err != nil {
		return interopengine.InteropState{}, err
	}
	return out, nil
}

func (s *Store) Commit(state interopengine.InteropState) error {
	encoded, err := encodeState(state)
	if err != nil {
		return err
	}
	raw, err := json.Marshal(encoded)
	if err != nil {
		return fmt.Errorf("marshal interop state: %w", err)
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		return b.Put([]byte(currentKey), raw)
	})
}

func (s *Store) Close() error {
	return s.db.Close()
}

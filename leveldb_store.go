package hatriecache

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

var levelDBEntryPrefix = []byte("entry:")

type LevelDBStore struct {
	db *leveldb.DB
}

func OpenLevelDBStore(path string) (*LevelDBStore, error) {
	if path == "" {
		return nil, errors.New("hatriecache: leveldb path is required")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	db, err := leveldb.OpenFile(path, &opt.Options{
		Compression: opt.SnappyCompression,
	})
	if err != nil {
		return nil, err
	}
	return &LevelDBStore{db: db}, nil
}

func (store *LevelDBStore) Close() error {
	if store == nil || store.db == nil {
		return nil
	}
	return store.db.Close()
}

func (store *LevelDBStore) Save(trie *HatTrie) error {
	entries, err := trie.levelDBEntries()
	if err != nil {
		return err
	}

	batch := new(leveldb.Batch)
	iterator := store.db.NewIterator(util.BytesPrefix(levelDBEntryPrefix), nil)
	for iterator.Next() {
		key := cloneBytes(iterator.Key())
		batch.Delete(key)
	}
	if err := iterator.Error(); err != nil {
		iterator.Release()
		return err
	}
	iterator.Release()

	for _, entry := range entries {
		data, err := json.Marshal(entry)
		if err != nil {
			return err
		}
		batch.Put(levelDBKey(entry.Key), data)
	}
	return store.db.Write(batch, &opt.WriteOptions{Sync: true})
}

func (store *LevelDBStore) Load(trie *HatTrie) (int, error) {
	iterator := store.db.NewIterator(util.BytesPrefix(levelDBEntryPrefix), nil)
	defer iterator.Release()

	now := trie.currentTime()
	operations := []snapshotOperation{}
	for iterator.Next() {
		entry, err := decodeLevelDBEntry(iterator.Value())
		if err != nil {
			return 0, err
		}
		if entry.ExpiresAt != nil && !now.Before(*entry.ExpiresAt) {
			continue
		}
		operation, err := validateSnapshotEntry(entry)
		if err != nil {
			return 0, err
		}
		operations = append(operations, operation)
	}
	if err := iterator.Error(); err != nil {
		return 0, err
	}

	for _, operation := range operations {
		if err := trie.applySnapshotOperation(operation); err != nil {
			return 0, err
		}
	}
	return len(operations), nil
}

func (trie *HatTrie) SaveLevelDB(path string) error {
	store, err := OpenLevelDBStore(path)
	if err != nil {
		return err
	}
	defer store.Close()
	return store.Save(trie)
}

func (trie *HatTrie) LoadLevelDB(path string) (int, error) {
	store, err := OpenLevelDBStore(path)
	if err != nil {
		return 0, err
	}
	defer store.Close()
	return store.Load(trie)
}

func (trie *HatTrie) levelDBEntries() ([]snapshotEntry, error) {
	trie.mu.Lock()
	defer trie.mu.Unlock()

	trie.ensureOpen()
	entries := trie.entriesWithPrefixLocked("", true)
	out := make([]snapshotEntry, 0, len(entries))
	for _, entry := range entries {
		snapshotEntry, err := trie.snapshotEntryLocked(entry)
		if err != nil {
			return nil, err
		}
		out = append(out, snapshotEntry)
	}
	return out, nil
}

func decodeLevelDBEntry(data []byte) (snapshotEntry, error) {
	var entry snapshotEntry
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&entry); err != nil {
		return snapshotEntry{}, err
	}
	var extra struct{}
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return snapshotEntry{}, errors.New("hatriecache: invalid leveldb entry JSON")
		}
		return snapshotEntry{}, err
	}
	return entry, nil
}

func levelDBKey(key string) []byte {
	out := make([]byte, 0, len(levelDBEntryPrefix)+len(key))
	out = append(out, levelDBEntryPrefix...)
	out = append(out, key...)
	return out
}

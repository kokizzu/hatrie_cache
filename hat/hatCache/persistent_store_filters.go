package hatCache

import (
	"fmt"
	"sync/atomic"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	leveldbfilter "github.com/syndtr/goleveldb/leveldb/filter"
)

const (
	// DefaultPersistentStoreBloomFilterBitsPerKey keeps the native run filter
	// disabled unless a read-heavy workload opts in.
	DefaultPersistentStoreBloomFilterBitsPerKey = 0
	MaxPersistentStoreBloomFilterBitsPerKey     = 64
)

var persistentStoreBloomFilterBitsPerKey atomic.Int32

// ConfigurePersistentStoreBloomFilterBitsPerKey configures native Bloom
// filters for stores opened after this call. Zero preserves the current
// no-filter behavior. Existing stores are not reconfigured.
func ConfigurePersistentStoreBloomFilterBitsPerKey(bitsPerKey int) error {
	if bitsPerKey < 0 || bitsPerKey > MaxPersistentStoreBloomFilterBitsPerKey {
		return fmt.Errorf("hatriecache: persistent store bloom filter bits/key must be between %d and %d", DefaultPersistentStoreBloomFilterBitsPerKey, MaxPersistentStoreBloomFilterBitsPerKey)
	}
	persistentStoreBloomFilterBitsPerKey.Store(int32(bitsPerKey))
	return nil
}

// PersistentStoreBloomFilterBitsPerKey returns the setting used by future
// persistent-store opens.
func PersistentStoreBloomFilterBitsPerKey() int {
	return int(persistentStoreBloomFilterBitsPerKey.Load())
}

func newPersistentStoreLevelDBFilter() leveldbfilter.Filter {
	bitsPerKey := PersistentStoreBloomFilterBitsPerKey()
	if bitsPerKey == DefaultPersistentStoreBloomFilterBitsPerKey {
		return nil
	}
	return leveldbfilter.NewBloomFilter(bitsPerKey)
}

func newPebbleStoreOptions(readOnly bool) *pebble.Options {
	options := (&pebble.Options{}).EnsureDefaults()
	options.ReadOnly = readOnly
	if bitsPerKey := PersistentStoreBloomFilterBitsPerKey(); bitsPerKey != DefaultPersistentStoreBloomFilterBitsPerKey {
		policy := bloom.FilterPolicy(bitsPerKey)
		for index := range options.Levels {
			options.Levels[index].FilterPolicy = policy
		}
	}
	return options
}

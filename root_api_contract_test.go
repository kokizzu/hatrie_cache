package hatriecache_test

import (
	"context"
	"testing"

	cache "hatrie_cache"
)

var _ func(context.Context, *cache.SQLConn, string, func(int) error) (int, error) = cache.QueryRows[int]

func TestRootAPIFacadeCoreContract(t *testing.T) {
	trie := cache.CreateHatTrie()
	defer trie.Destroy()

	stored := trie.ExecuteCommand(cache.CacheCommandRequest{Command: "SETSTR", Key: "facade:key", Value: "value"})
	if !stored.OK {
		t.Fatalf("SETSTR response = %#v", stored)
	}
	got := trie.ExecuteCommand(cache.CacheCommandRequest{Command: "GETSTR", Key: "facade:key"})
	if !got.OK || got.Value != "value" {
		t.Fatalf("GETSTR response = %#v", got)
	}

	if format, err := cache.ParseCommandWireFormat("json"); err != nil || format != cache.CommandWireFormatJSON {
		t.Fatalf("ParseCommandWireFormat(json) = %q/%v", format, err)
	}
	if format, err := cache.ParseSnapshotFormat("binary"); err != nil || format != cache.SnapshotFormatBinary {
		t.Fatalf("ParseSnapshotFormat(binary) = %q/%v", format, err)
	}
	if backend, err := cache.ParseStorageBackend("pebble"); err != nil || backend != cache.StorageBackendPebble {
		t.Fatalf("ParseStorageBackend(pebble) = %q/%v", backend, err)
	}
}

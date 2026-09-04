package hatriecache_test

import (
	"testing"

	hatriecache "hatrie_cache"
)

func TestTokenBloomFilterRootAPIIsImportable(t *testing.T) {
	filter, err := hatriecache.NewTokenBloomFilterWithShape(1024, 3)
	if err != nil {
		t.Fatalf("NewTokenBloomFilterWithShape() error = %v", err)
	}
	filter.AddText("root package")
	if !filter.ContainsAllTokens("ROOT package") {
		t.Fatal("root package alias lost token membership")
	}
}

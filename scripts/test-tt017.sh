#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^Test(PersistentStoreRunFilterPreservesEntrySemantics|PersistentStoreRunFilterReopenPreservesEntrySemantics|TT017PersistentStoreFilterStorageFootprint|PersistentStoreBloomFilterConfiguration)$' -count=1 -v
go test ./cmd/hatrie-cache -run '^TestParseConfigStorageBloomFilter(ValidatesRange)?$' -count=1

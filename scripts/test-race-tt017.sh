#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatCache ./cmd/hatrie-cache -run 'Test(PersistentStoreRunFilterPreservesEntrySemantics|PersistentStoreRunFilterReopenPreservesEntrySemantics|TT017PersistentStoreFilterStorageFootprint|PersistentStoreBloomFilterConfiguration|ParseConfigStorageBloomFilter(ValidatesRange)?$)' -count=1

#!/usr/bin/env sh
set -eu

go test ./hat/hatCache ./cmd/hatrie-cache -run '^(TestCompactMemoryRefusesOverBudgetBeforeMutating|TestCompactMemoryReportsNativeTrieBytes|TestCompactMemoryRejectsNilTrie|TestMemoryCompactor|TestParseConfig)' -count=1

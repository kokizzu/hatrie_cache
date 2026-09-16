#!/usr/bin/env bash
set -euo pipefail

repo_root=$(pwd)
tmp_root=$(mktemp -d /tmp/hatrie-cache-chu13-race.XXXXXX)
cleanup() {
	git -C "$repo_root" worktree remove --force "$tmp_root" >/dev/null 2>&1 || true
	rm -rf "$tmp_root"
}
trap cleanup EXIT

git -C "$repo_root" worktree add --detach "$tmp_root" HEAD >/dev/null
cp "$repo_root/hat/hatDataStructure/ch025_token_postings_index.go" "$tmp_root/hat/hatDataStructure/ch025_token_postings_index.go"
cp "$repo_root/hat/hatDataStructure/ch_u13_phrase_postings_index.go" "$tmp_root/hat/hatDataStructure/ch_u13_phrase_postings_index.go"
cp "$repo_root/hat/hatDataStructure/ch_u13_phrase_postings_index_test.go" "$tmp_root/hat/hatDataStructure/ch_u13_phrase_postings_index_test.go"
cp "$repo_root/hat/hatDataStructure/ch_u13_phrase_postings_index_lifecycle_test.go" "$tmp_root/hat/hatDataStructure/ch_u13_phrase_postings_index_lifecycle_test.go"
cp "$repo_root/hat/hatDataStructure/ch_u13_phrase_postings_index_baseline_benchmark_test.go" "$tmp_root/hat/hatDataStructure/ch_u13_phrase_postings_index_baseline_benchmark_test.go"
cp "$repo_root/hat/hatDataStructure/ch_u13_phrase_postings_index_benchmark_test.go" "$tmp_root/hat/hatDataStructure/ch_u13_phrase_postings_index_benchmark_test.go"

cd "$tmp_root"
go test -race ./hat/hatDataStructure -run '^Test(CHU13|CH025TokenPostingsIndex)' -count=1

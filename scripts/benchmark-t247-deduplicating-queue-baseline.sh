#!/usr/bin/env bash
set -euo pipefail

test_file=hat/hatDataStructure/t247_deduplicating_priority_visibility_queue_test.go
backup=$(mktemp /tmp/hatrie-t247-test.XXXXXX)
cp "$test_file" "$backup"
cleanup() {
	mv "$backup" "$test_file"
}
trap cleanup EXIT
rm "$test_file"

go test ./hat/hatDataStructure -run '^$' -bench '^BenchmarkPriorityVisibilityQueueLeaseAck$' -benchmem -count=5

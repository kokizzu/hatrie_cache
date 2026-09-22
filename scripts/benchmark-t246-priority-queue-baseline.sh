#!/usr/bin/env bash
set -euo pipefail

test_file=hat/hatDataStructure/t246_priority_visibility_queue_test.go
backup=$(mktemp /tmp/hatrie-t246-test.XXXXXX)
cp "$test_file" "$backup"
cleanup() {
	mv "$backup" "$test_file"
}
trap cleanup EXIT
rm "$test_file"

go test ./hat/hatDataStructure -run '^$' -bench '^(BenchmarkPriorityVisibilityQueueLeaseAck|BenchmarkVisibilityQueueLeaseAckBaseline)$' -benchmem -count=5

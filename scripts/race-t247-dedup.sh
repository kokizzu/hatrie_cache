#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatDataStructure/t247_dedup_queue.go ./hat/hatDataStructure/t247_dedup_queue_test.go -run '^TestDeduplicatingQueue'

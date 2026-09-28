#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure/t247_dedup_queue.go ./hat/hatDataStructure/t247_dedup_queue_test.go -run '^TestDeduplicatingQueue'

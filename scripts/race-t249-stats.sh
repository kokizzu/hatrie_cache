#!/usr/bin/env bash
set -euo pipefail

go test -race \
  -run '^TestDeadLetterQueueStats' \
  -count=1 \
  -timeout=120s \
  ./hat/hatDataStructure/delay_queue.go \
  ./hat/hatDataStructure/dead_letter_queue.go \
  ./hat/hatDataStructure/queue_stats.go \
  ./hat/hatDataStructure/t249_queue_stats_test.go

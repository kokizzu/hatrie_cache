#!/usr/bin/env bash
set -euo pipefail

go test \
  -run '^TestDeadLetterQueueStats' \
  -count=1 \
  -timeout=60s \
  ./hat/hatDataStructure/delay_queue.go \
  ./hat/hatDataStructure/dead_letter_queue.go \
  ./hat/hatDataStructure/queue_stats.go \
  ./hat/hatDataStructure/t249_queue_stats_test.go

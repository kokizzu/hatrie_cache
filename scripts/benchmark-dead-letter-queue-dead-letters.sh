#!/usr/bin/env bash
set -euo pipefail

go test hat/hatDataStructure/delay_queue.go \
    hat/hatDataStructure/dead_letter_queue.go \
    hat/hatDataStructure/dead_letter_queue_dead_letters_benchmark_test.go \
    -run '^$' -bench '^BenchmarkDeadLetterQueueDeadLetters$' -benchmem -count=10

#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatDataStructure/delay_queue.go hat/hatDataStructure/delay_queue_pop_ready_fastpath_test.go

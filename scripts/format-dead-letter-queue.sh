#!/bin/sh
set -eu

gofmt -w hat/hatDataStructure/dead_letter_queue.go hat/hatDataStructure/dead_letter_queue_test.go hat/hatDataStructure/dead_letter_queue_benchmark_test.go

#!/bin/sh
set -eu

gofmt -w \
  hat/hatDataStructure/visibility_queue.go \
  hat/hatDataStructure/visibility_queue_epoch_test.go \
  hat/hatDataStructure/visibility_queue_epoch_benchmark_test.go

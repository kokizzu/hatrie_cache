#!/bin/sh
set -eu

gofmt -w \
  hat/hatDataStructure/delay_queue.go \
  hat/hatDataStructure/delay_queue_test.go \
  hat/hatDataStructure/delay_queue_benchmark_test.go

for file in \
  Makefile \
  DELAY_QUEUE.md \
  scripts/benchmark-delay-queue.sh \
  scripts/format-delay-queue.sh \
  scripts/test-delay-queue.sh \
  scripts/test-race-delay-queue.sh \
  scripts/vet-delay-queue.sh
do
  while [ "$(tail -n 1 "$file")" = "" ]; do
    sed -i '${/^$/d;}' "$file"
  done
done

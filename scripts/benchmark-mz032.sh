#!/bin/sh
set -eu

GOMAXPROCS=1 go test ./hat/hatSql \
  -run '^$' \
  -bench '^BenchmarkMZ032ManualArrangementCost$' \
  -benchmem \
  -count=5 \
  -benchtime=1000x

GOMAXPROCS=1 go test ./hat/hatSql \
  -run '^$' \
  -bench '^BenchmarkMZ032SQLArrangementCostModel$' \
  -benchmem \
  -count=5 \
  -benchtime=1000x

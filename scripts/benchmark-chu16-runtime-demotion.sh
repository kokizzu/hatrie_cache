#!/bin/sh
set -eu

go test ./hat/hatSql \
  -run '^$' \
  -bench '^BenchmarkCHU16Adaptive(LongChurn|PromotionThenChurn)$' \
  -benchmem \
  -count=5

#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
    hat/hatSql/arg_extreme_state.go \
    hat/hatSql/ch037_arg_extreme_state_test.go \
    hat/hatSql/ch037_arg_extreme_state_codec_test.go \
    hat/hatSql/ch037_arg_extreme_state_benchmark_test.go \
    hat/hatSql/query.go

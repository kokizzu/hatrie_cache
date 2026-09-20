#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/index_hint.go hat/hatSql/index_strategy.go hat/hatSql/contracts.go hat/hatSql/tu26_index_strategy_test.go

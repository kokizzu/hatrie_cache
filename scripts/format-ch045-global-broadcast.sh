#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/ch045_global_join_broadcast_plan.go hat/hatSql/ch045_global_join_broadcast_plan_test.go

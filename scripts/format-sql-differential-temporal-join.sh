#!/usr/bin/env bash
set -euo pipefail

gofmt -w ./hat/hatSql/differential_temporal_join.go ./hat/hatSql/differential_temporal_join_test.go

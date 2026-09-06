#!/usr/bin/env bash
set -euo pipefail

gofmt -w ./hat/hatSql/differential_group_by.go ./hat/hatSql/differential_group_by_test.go

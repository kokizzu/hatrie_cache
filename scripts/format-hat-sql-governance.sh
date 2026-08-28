#!/usr/bin/env sh
set -eu

gofmt -w ./hat/hatSql/governance.go ./hat/hatSql/governance_test.go

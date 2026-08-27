#!/bin/sh
set -eu

cd "$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
gofmt -w ./hat/hatSql/tooling.go ./hat/hatSql/tooling_test.go

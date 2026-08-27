#!/bin/sh
set -eu

cd "$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
gofmt -w ./hat/hatSql/fixture.go ./hat/hatSql/fixture_test.go

#!/bin/sh
set -eu

cd "$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
go test ./hat/hatSql -run '^TestEmbeddedQueryFixture' -count=1

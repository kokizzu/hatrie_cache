#!/usr/bin/env bash
set -euo pipefail
trap 'make cleanup-hatrie-tmp-after-test' EXIT

go test -race -tags=t217 ./hat/hatSql -run 'TestTypedTableAppendColumnarBatch' -count=1

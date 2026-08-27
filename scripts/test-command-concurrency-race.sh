#!/bin/sh
set -eu

cd "$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
go test -race ./hat/hatCache -run '^TestExecuteCommandConcurrent' -count=1

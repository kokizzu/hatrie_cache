#!/bin/sh
set -eu

cd "$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
go test ./hat/hatCache -run '^TestExecuteCommandConcurrent' -count=1

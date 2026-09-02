#!/bin/sh
set -eu
go test ./hat/hatSql -run '^TestTypedTable(PatchParts|LightweightDeletes)' -count=1

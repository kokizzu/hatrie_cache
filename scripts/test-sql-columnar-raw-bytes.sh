#!/usr/bin/env sh
set -eu

go test ./hat/hatCache -run '^TestSQLColumnarRawBytesBatchUsesLockedRawStorage$' -count=1

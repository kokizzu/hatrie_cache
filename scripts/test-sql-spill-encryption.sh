#!/bin/sh
set -eu

go test ./hat/hatSql -run '^TestEncryptedSQLSortSpillHidesPlaintextAndExecutes$' -count=1

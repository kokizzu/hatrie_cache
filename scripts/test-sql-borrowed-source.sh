#!/bin/sh
set -eu

go test ./hat/hatSql -run '^TestSQLSourceResolverUsesBorrowedImmutableSnapshotWhenAvailable$' -count=1

#!/usr/bin/env bash
set -euo pipefail

make test-sql-multikey
go test ./hat/hatDataStructure -run 'Test(StringMultikeyIndex|TupleMultikeyIndex)' -count=1

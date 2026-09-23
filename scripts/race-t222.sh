#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatCache -run 'Multikey' -count=1
go test -race ./hat/hatDataStructure -run 'Test(StringMultikeyIndex|TupleMultikeyIndex)' -count=1

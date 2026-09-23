#!/usr/bin/env bash
set -euo pipefail

make test-sql-partial-index
go test ./hat/hatDataStructure -run 'Test(ConditionalFunctionalIndex|TU24ConditionalIndexCatalog)' -count=1

#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatCache -run 'PartialIndex'
go test -race ./hat/hatDataStructure -run 'Test(ConditionalFunctionalIndex|TU24ConditionalIndexCatalog)' -count=1

#!/usr/bin/env bash
set -euo pipefail

go test hat/hatDataStructure/differential_multiset.go \
    hat/hatDataStructure/logical_compaction.go \
    hat/hatDataStructure/logical_compaction_records_into_test.go
go test -race hat/hatDataStructure/differential_multiset.go \
    hat/hatDataStructure/logical_compaction.go \
    hat/hatDataStructure/logical_compaction_records_into_test.go

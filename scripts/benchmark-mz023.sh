#!/bin/sh
set -eu
GOMAXPROCS=1 go test ./hat/hatSql -run '^$' -bench '^(BenchmarkSQLSinkCommitCoordinator(NewCommit|DuplicateCommit)|BenchmarkSQLSinkCommitCoordinatorAuditedCommit|BenchmarkSQLSinkDeliveryAuditMarshalBinary)$' -benchmem -benchtime=100ms -count=5 -timeout 120s

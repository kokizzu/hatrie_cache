#!/bin/sh
set -eu

git add \
	Makefile \
	ENGINE_IDEAS.md \
	BENCHMARK.md \
	TT007_SNAPSHOT_WAL_JOIN.md \
	hat/hatDataStructure/tu19_tuple_operation_journal.go \
	hat/hatDataStructure/tt007_snapshot_wal_join_test.go \
	hat/hatDataStructure/tt007_snapshot_wal_join_benchmark_test.go \
	scripts/tt007-snapshot-wal-join.sh \
	scripts/stage-tt007-snapshot-wal-join.sh \
	scripts/commit-tt007-snapshot-wal-join.sh \
	scripts/push-tt007-snapshot-wal-join.sh

git status --short

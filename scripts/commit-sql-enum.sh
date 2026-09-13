#!/bin/sh
set -eu

git add \
	BENCHMARK.md \
	INSPIRATION_BACKLOG.md \
	Makefile \
	README.md \
	SQL_ENUM_TYPES.md \
	hat/hatSchema/compatibility.go \
	hat/hatSchema/enum_test.go \
	hat/hatSchema/fingerprint.go \
	hat/hatSchema/generate.go \
	hat/hatSchema/schema.go \
	hat/hatSql/enum_row_binary_benchmark_test.go \
	hat/hatSql/enum_row_binary_test.go \
	hat/hatSql/row_binary.go \
	hat/hatSql/row_binary_delta_codec.go \
	hat/hatSql/row_binary_enum.go \
	hat/hatSql/row_binary_nullable_bitmap.go \
	hat/hatSql/row_binary_read_stats.go \
	hat/hatSql/row_binary_stats.go \
	hat/hatSql/row_binary_stats_pruning.go \
	hat/hatSql/row_binary_stream.go \
	scripts/audit-next-inspiration.sh \
	scripts/benchmark-sql-enum-after.sh \
	scripts/benchmark-sql-enum-before.sh \
	scripts/benchmark-sql-enum-comparison.sh \
	scripts/benchmark-sql-enum-default.sh \
	scripts/benchmark-sql-enum-encode.sh \
	scripts/commit-sql-enum.sh \
	scripts/format-sql-enum.sh \
	scripts/inspect-ch035.sh \
	scripts/push-sql-enum.sh \
	scripts/race-sql-enum.sh \
	scripts/review-sql-enum.sh \
	scripts/test-sql-enum-full.sh \
	scripts/test-sql-enum.sh \
	scripts/vet-sql-enum.sh
git commit -m "feat: add compact SQL enum rowbinary values"

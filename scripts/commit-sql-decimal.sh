#!/bin/sh
set -eu

git add Makefile README.md BENCHMARK.md INSPIRATION_BACKLOG.md SQL_DECIMAL_TYPES.md \
	hat/hatSchema/compatibility.go hat/hatSchema/fingerprint.go hat/hatSchema/generate.go \
	hat/hatSchema/schema.go hat/hatSchema/decimal_test.go \
	hat/hatSql/query.go hat/hatSql/row_binary.go hat/hatSql/row_binary_delta_codec.go \
	hat/hatSql/row_binary_dictionary.go \
	hat/hatSql/row_binary_enum.go hat/hatSql/row_binary_nullable_bitmap.go \
	hat/hatSql/row_binary_read_stats.go hat/hatSql/row_binary_stats.go \
	hat/hatSql/row_binary_stream.go hat/hatSql/decimal_row_binary_benchmark_test.go \
	hat/hatSql/decimal_types.go hat/hatSql/decimal_types_test.go \
	scripts/benchmark-sql-decimal-after.sh scripts/benchmark-sql-decimal-before.sh \
	scripts/format-sql-decimal.sh scripts/inspect-benchmark-ch032.sh \
	scripts/inspect-ch032-dictionary.sh \
	scripts/inspect-ch032-decimal-head.sh scripts/inspect-ch032-decimal-test.sh \
	scripts/inspect-ch032-stream-head.sh \
	scripts/inspect-ch032-uses.sh scripts/inspect-ch032.sh scripts/inspect-readme-ch032.sh \
	scripts/review-sql-decimal.sh scripts/race-sql-decimal.sh \
	scripts/test-sql-decimal-full.sh scripts/test-sql-decimal.sh scripts/vet-sql-decimal.sh \
	scripts/commit-sql-decimal.sh scripts/git-context-sql-decimal.sh scripts/push-sql-decimal.sh
git commit -m "feat: add fixed-width SQL decimal rowbinary values"

#!/bin/sh
set -eu

git add -- \
	BENCHMARK.md \
	ENGINE_IDEAS.md \
	Makefile \
	README.md \
	SQL_QUOTAS.md \
	hat/hatCache/sql_quota.go \
	hat/hatSql/ch029_sql_quota_benchmark_test.go \
	hat/hatSql/ch029_sql_quota_test.go \
	hat/hatSql/query.go \
	hat/hatSql/quota.go \
	scripts/benchmark-ch029-sql-quotas.sh \
	scripts/commit-ch029-sql-quotas.sh \
	scripts/format-ch029-sql-quotas.sh \
	scripts/push-ch029-sql-quotas.sh \
	scripts/race-ch029-sql-quotas.sh \
	scripts/review-ch029-sql-quotas.sh \
	scripts/test-ch029-sql-quotas.sh \
	scripts/verify-ch029-docs.sh \
	scripts/vet-ch029-sql-quotas.sh
git diff --cached --check
git commit -m 'feat: add keyed SQL quotas'

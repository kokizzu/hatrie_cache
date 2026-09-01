#!/bin/sh
set -eu

mode=${1:-plan}

stage_makefile_targets() {
	base=$(mktemp)
	desired=$(mktemp)
	trap 'rm -f "$base" "$desired"' EXIT HUP INT TERM
	git show :Makefile >"$base"
	if grep -Fqx 'benchmark-sql-typed-table-join:' "$base"; then
		return
	fi
	awk '
		$0 == "test-sql-typed-table:" {
			print
			getline
			print
			print ""
			print ".PHONY: benchmark-sql-typed-table-join"
			print "benchmark-sql-typed-table-join:"
			print "\tsh ./scripts/benchmark-sql-typed-table-join.sh"
			print ""
			print ".PHONY: deliver-sql-typed-table-join-arrangements"
			print "deliver-sql-typed-table-join-arrangements:"
			print "\tsh ./scripts/deliver-sql-typed-table-join-arrangements.sh plan"
			print ""
			print ".PHONY: verify-sql-typed-table-join-arrangements-delivery"
			print "verify-sql-typed-table-join-arrangements-delivery:"
			print "\tsh ./scripts/deliver-sql-typed-table-join-arrangements.sh verify"
			print ""
			print ".PHONY: apply-sql-typed-table-join-arrangements-delivery"
			print "apply-sql-typed-table-join-arrangements-delivery:"
			print "\tsh ./scripts/deliver-sql-typed-table-join-arrangements.sh apply"
			print ""
			print ".PHONY: unstage-sql-typed-table-join-arrangements-delivery"
			print "unstage-sql-typed-table-join-arrangements-delivery:"
			print "\tsh ./scripts/deliver-sql-typed-table-join-arrangements.sh unstage"
			inserted = 1
			next
		}
		{ print }
		END { if (!inserted) exit 1 }
	' "$base" >"$desired"
	hash=$(git hash-object -w "$desired")
	git update-index --add --cacheinfo "100644,$hash,Makefile"
}

case "$mode" in
plan)
	git status --short
	git diff --cached --name-only
	git diff --check -- ADOPTED_QUERY_ENGINE_IDEAS.md TYPED_TABLES.md hat/hatSql/typed_table_join.go hat/hatSql/typed_table_join_arrangements.go hat/hatSql/typed_table_join_benchmark_test.go hat/hatSql/typed_table_join_test.go scripts/benchmark-sql-typed-table-join.sh scripts/deliver-sql-typed-table-join-arrangements.sh
	;;
verify)
	git diff --cached --check
	git diff --cached --name-only
	;;
apply)
	if ! git diff --cached --quiet; then
		echo "refusing to deliver with pre-existing staged changes" >&2
		exit 1
	fi
	stage_makefile_targets
	git add -- ADOPTED_QUERY_ENGINE_IDEAS.md TYPED_TABLES.md hat/hatSql/typed_table_join.go hat/hatSql/typed_table_join_arrangements.go hat/hatSql/typed_table_join_benchmark_test.go hat/hatSql/typed_table_join_test.go scripts/benchmark-sql-typed-table-join.sh scripts/deliver-sql-typed-table-join-arrangements.sh
	git diff --cached --check
	git commit -m "perf: remove typed join pair key allocation"
	git push origin master
	;;
unstage)
	git restore --staged -- ADOPTED_QUERY_ENGINE_IDEAS.md TYPED_TABLES.md Makefile hat/hatSql/typed_table_join.go hat/hatSql/typed_table_join_arrangements.go hat/hatSql/typed_table_join_benchmark_test.go hat/hatSql/typed_table_join_test.go scripts/benchmark-sql-typed-table-join.sh scripts/deliver-sql-typed-table-join-arrangements.sh
	;;
*)
	echo "usage: $0 [plan|verify|apply|unstage]" >&2
	exit 2
	;;
esac

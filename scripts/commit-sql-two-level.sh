#!/bin/sh
set -eu

mode="${1:-inspect}"
root="$(git rev-parse --show-toplevel)"
cd "$root"

feature_paths='INSPIRATION.md SQL_TWO_LEVEL_AGGREGATION.md README.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md hat/hatSql/columnar_vector_group_aggregate.go hat/hatSql/hash_group_aggregate.go hat/hatSql/two_level_group_aggregate_test.go hat/hatSql/two_level_group_aggregate_benchmark_test.go scripts/verify-inspiration.sh scripts/test-sql-two-level.sh scripts/benchmark-sql-two-level.sh scripts/benchmark-sql-two-level-before.sh scripts/format-sql-two-level.sh scripts/test-race-sql-two-level.sh scripts/vet-sql-two-level.sh scripts/commit-sql-two-level.sh'

stage_makefile_targets() {
  if git diff --cached -- Makefile | rg -q '^\+stage-sql-two-level:'; then
    return
  fi

  stage_dir="$(mktemp -d /tmp/hatrie-two-level-stage.XXXXXX)"
	git show HEAD:Makefile > "$stage_dir/base"
	awk '
	{
		print
		if ($0 == "\tsh ./scripts/commit-sql-vectorized.sh push") {
			print ""
			print "verify-inspiration:"
			print "\tsh ./scripts/verify-inspiration.sh"
			print ""
			print "test-sql-two-level:"
			print "\tsh ./scripts/test-sql-two-level.sh"
			print ""
			print "benchmark-sql-two-level:"
			print "\tsh ./scripts/benchmark-sql-two-level.sh"
			print ""
			print "format-sql-two-level:"
			print "\tsh ./scripts/format-sql-two-level.sh"
			print ""
			print "test-race-sql-two-level:"
			print "\tsh ./scripts/test-race-sql-two-level.sh"
			print ""
			print "vet-sql-two-level:"
			print "\tsh ./scripts/vet-sql-two-level.sh"
			print ""
			print "benchmark-sql-two-level-before:"
			print "\tsh ./scripts/benchmark-sql-two-level-before.sh"
			print ""
			print "benchmark-sql-two-level-long:"
			print "\tBENCHTIME=3s sh ./scripts/benchmark-sql-two-level.sh"
			print ""
			print "benchmark-sql-two-level-before-long:"
			print "\tBENCHTIME=3s sh ./scripts/benchmark-sql-two-level-before.sh"
			print ""
			print "stage-sql-two-level:"
			print "\tsh ./scripts/commit-sql-two-level.sh stage"
			print ""
			print "commit-sql-two-level:"
			print "\tsh ./scripts/commit-sql-two-level.sh commit"
			print ""
			print "push-sql-two-level:"
			print "\tsh ./scripts/commit-sql-two-level.sh push"
		}
	}' "$stage_dir/base" > "$stage_dir/desired"
	if diff -u --label a/Makefile --label b/Makefile "$stage_dir/base" "$stage_dir/desired" > "$stage_dir/patch"; then
		:
	else
		status=$?
		test "$status" -eq 1
	fi
	git apply --cached "$stage_dir/patch"
	rm -rf "$stage_dir"
}

stage_feature() {
	git add -- $feature_paths
	stage_makefile_targets
	git diff --cached --check
}

case "$mode" in
inspect)
	git status --short
	git diff --stat -- $feature_paths
	git diff --check -- $feature_paths
	;;
stage)
	stage_feature
	git diff --cached --stat
	;;
commit)
	stage_feature
	git commit -m "sql: add opt-in two-level aggregation inspiration"
	;;
push)
	git push origin HEAD
	;;
*)
	printf 'usage: %s {inspect|stage|commit|push}\n' "$0" >&2
	exit 2
	;;
esac

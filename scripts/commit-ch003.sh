#!/usr/bin/env bash
set -euo pipefail

mode="${1:-commit}"
case "$mode" in
status)
	git status --short
	git rev-parse --short HEAD
	;;
commit)
	git add \
		ADOPTED_QUERY_ENGINE_IDEAS.md \
		BENCHMARK.md \
		INSPIRATION_BACKLOG.md \
		Makefile \
		README.md \
		SQL_NAMESPACE_ADMISSION.md \
		hat/hatSql/ch003_resource_profile_baseline_benchmark_test.go \
		hat/hatSql/ch003_resource_profile_benchmark_test.go \
		hat/hatSql/ch003_resource_profile_test.go \
		hat/hatSql/governance.go \
		scripts/commit-ch003.sh \
		scripts/push-ch003.sh \
		scripts/test-ch003.sh
	git commit -m "feat(hatSql): add namespace admission previews"
	;;
*)
	printf 'usage: %s [status|commit]\n' "$0" >&2
	exit 2
	;;
esac

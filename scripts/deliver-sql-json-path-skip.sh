#!/bin/sh
set -eu

mode=${1:-check}
feature_paths="ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md hat/hatCache/main.go hat/hatCache/sql_json_path_skip.go hat/hatCache/sql_json_path_skip_test.go hat/hatCache/sql_json_path_skip_benchmark_test.go hat/hatCache/sql_query.go scripts/benchmark-sql-json-path-skip.sh scripts/deliver-sql-json-path-skip.sh scripts/format-sql-json-path-skip.sh scripts/test-race-sql-json-path-skip.sh scripts/test-sql-json-path-skip.sh"

stage_makefile_targets() {
	base=$(mktemp)
	patch=$(mktemp)
	trap 'rm -f "$base" "$patch"' EXIT HUP INT TERM
	git show HEAD:Makefile > "$base"
	if grep -q '^test-sql-json-path-skip:' "$base"; then
		echo "JSON path skip targets already exist in HEAD" >&2
		exit 1
	fi
	line_count=$(wc -l < "$base")
	cat > "$patch" <<PATCH
diff --git a/Makefile b/Makefile
--- a/Makefile
+++ b/Makefile
@@ -${line_count},0 +${line_count},16 @@
+
+.PHONY: test-sql-json-path-skip
+test-sql-json-path-skip:
+	bash scripts/test-sql-json-path-skip.sh
+
+.PHONY: benchmark-sql-json-path-skip
+benchmark-sql-json-path-skip:
+	bash scripts/benchmark-sql-json-path-skip.sh
+
+.PHONY: format-sql-json-path-skip
+format-sql-json-path-skip:
+	bash scripts/format-sql-json-path-skip.sh
+
+.PHONY: test-race-sql-json-path-skip
+test-race-sql-json-path-skip:
+	bash scripts/test-race-sql-json-path-skip.sh
PATCH
	git apply --cached --unidiff-zero "$patch"
}

case "$mode" in
check)
	git status --short
	git diff --check -- $feature_paths Makefile
	git diff --stat -- $feature_paths Makefile
	;;
apply)
	if ! git diff --cached --quiet; then
		echo "refusing to stage with pre-existing staged changes" >&2
		exit 1
	fi
	git diff --check -- $feature_paths Makefile
	git add $feature_paths
	stage_makefile_targets
	git diff --cached --check
	git diff --cached --name-only
	;;
commit)
	git diff --cached --check
	git commit -m "feat: add bounded JSON path skip metadata"
	;;
push)
	git push origin HEAD
	;;
*)
	echo "usage: $0 {check|apply|commit|push}" >&2
	exit 2
	;;
esac

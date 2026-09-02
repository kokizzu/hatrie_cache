#!/bin/sh
set -eu

mode=${1:-check}
feature_paths="ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md TYPED_TABLES.md hat/hatSql/typed_table.go hat/hatSql/typed_table_patch_parts.go hat/hatSql/typed_table_patch_parts_test.go hat/hatSql/typed_table_patch_parts_benchmark_test.go scripts/benchmark-sql-typed-table-patch-parts.sh scripts/deliver-sql-typed-table-patch-parts.sh scripts/format-sql-typed-table-patch-parts.sh scripts/test-sql-typed-table-patch-parts.sh"

stage_makefile_targets() {
	base=$(mktemp)
	patch=$(mktemp)
	trap 'rm -f "$base" "$patch"' EXIT HUP INT TERM
	git show HEAD:Makefile > "$base"
	if grep -q '^test-sql-typed-table-patch-parts:' "$base"; then
		echo "typed-table patch targets already exist in HEAD" >&2
		exit 1
	fi
	line_count=$(wc -l < "$base")
	cat > "$patch" <<PATCH
diff --git a/Makefile b/Makefile
--- a/Makefile
+++ b/Makefile
@@ -${line_count},0 +${line_count},26 @@
+test-sql-typed-table-patch-parts:
+	bash scripts/test-sql-typed-table-patch-parts.sh
+
+.PHONY: format-sql-typed-table-patch-parts
+format-sql-typed-table-patch-parts:
+	bash scripts/format-sql-typed-table-patch-parts.sh
+
+.PHONY: deliver-sql-typed-table-patch-parts
+deliver-sql-typed-table-patch-parts:
+	bash scripts/deliver-sql-typed-table-patch-parts.sh apply
+
+.PHONY: check-sql-typed-table-patch-parts
+check-sql-typed-table-patch-parts:
+	bash scripts/deliver-sql-typed-table-patch-parts.sh check
+
+.PHONY: commit-sql-typed-table-patch-parts
+commit-sql-typed-table-patch-parts:
+	bash scripts/deliver-sql-typed-table-patch-parts.sh commit
+
+.PHONY: push-sql-typed-table-patch-parts
+push-sql-typed-table-patch-parts:
+	bash scripts/deliver-sql-typed-table-patch-parts.sh push
+
+.PHONY: benchmark-sql-typed-table-patch-parts
+benchmark-sql-typed-table-patch-parts:
+	bash scripts/benchmark-sql-typed-table-patch-parts.sh
PATCH
	git apply --cached --unidiff-zero "$patch"
}

case "$mode" in
check)
	git status --short
	git diff --check -- $feature_paths Makefile
	git diff --stat -- $feature_paths Makefile
	git diff --unified=1 -- Makefile
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
	git commit -m "feat: add lightweight typed-table delete patches"
	;;
push)
	git push origin HEAD
	;;
*)
	echo "usage: $0 {check|apply|commit|push}" >&2
	exit 2
	;;
esac

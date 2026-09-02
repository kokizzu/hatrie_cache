#!/bin/sh
set -eu

paths='ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md TYPED_TABLES.md scripts/deliver-query-engine-adoption-docs.sh'

case "${1:-apply}" in
check)
	git diff --check
	git diff --cached --check
	git diff --cached --name-only -- $paths Makefile
	;;
apply)
	git diff --check
	git add $paths
	if ! git grep --cached -q '^deliver-query-engine-adoption-docs:$' -- Makefile; then
		git apply --cached --unidiff-zero --whitespace=nowarn <<'PATCH'
diff --git a/Makefile b/Makefile
--- a/Makefile
+++ b/Makefile
@@ -3377,0 +3378,4 @@
+deliver-query-engine-adoption-docs:
+	bash scripts/deliver-query-engine-adoption-docs.sh apply
+check-query-engine-adoption-docs-stage:
+	bash scripts/deliver-query-engine-adoption-docs.sh check
PATCH
	fi
	git diff --cached --check
	git diff --cached --name-only -- $paths Makefile
	git commit -m 'docs: record query engine adoption status'
	git push
	;;
*)
	printf '%s\n' 'usage: deliver-query-engine-adoption-docs.sh [apply|check]' >&2
	exit 2
	;;
esac

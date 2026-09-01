#!/bin/sh
set -eu

case "${1:-apply}" in
check)
	git diff --check
	git diff --cached --check
	;;
apply)
	git diff --check
	git add ADOPTED_QUERY_ENGINE_IDEAS.md TYPED_TABLES.md hat/hatSql/typed_table.go hat/hatSql/typed_table_dictionary_storage_benchmark_test.go hat/hatSql/typed_table_dictionary_storage_test.go scripts/test-sql-typed-dictionary-storage.sh scripts/benchmark-sql-typed-dictionary-storage.sh scripts/verify-sql-typed-dictionary-storage.sh scripts/deliver-sql-typed-dictionary-storage-plan.sh scripts/deliver-sql-typed-dictionary-storage.sh
	if ! git grep --cached -q '^deliver-sql-typed-dictionary-storage:$' -- Makefile; then
		git apply --cached --unidiff-zero --whitespace=nowarn <<'PATCH'
diff --git a/Makefile b/Makefile
--- a/Makefile
+++ b/Makefile
@@ -3370,0 +3371,16 @@
+
+test-sql-typed-dictionary-storage:
+	sh ./scripts/test-sql-typed-dictionary-storage.sh
+
+benchmark-sql-typed-dictionary-storage:
+	sh ./scripts/benchmark-sql-typed-dictionary-storage.sh
+verify-sql-typed-dictionary-storage:
+	sh ./scripts/verify-sql-typed-dictionary-storage.sh
+
+deliver-sql-typed-dictionary-storage-plan:
+	sh ./scripts/deliver-sql-typed-dictionary-storage-plan.sh
+
+deliver-sql-typed-dictionary-storage:
+	sh ./scripts/deliver-sql-typed-dictionary-storage.sh apply
+check-sql-typed-dictionary-storage-stage:
+	sh ./scripts/deliver-sql-typed-dictionary-storage.sh check
PATCH
	fi
	git diff --cached --check
	git commit -m 'feat: dictionary encode typed strings'
	git push
	;;
*) exit 2 ;;
esac

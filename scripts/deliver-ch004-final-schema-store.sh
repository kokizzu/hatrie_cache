#!/usr/bin/env bash
set -euo pipefail

root_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
temporary_root=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-ch004-final-store-deliver.XXXXXX")
trap 'rm -rf "$temporary_root"' EXIT
cd "$root_dir"

if git diff --cached --quiet; then
	:
else
	printf '%s\n' 'refusing delivery: the index already contains staged changes' >&2
	exit 1
fi

git show HEAD:Makefile > "$temporary_root/Makefile.base"
awk '
BEGIN { added = 0 }
{ print }
END {
    print ""
    print ".PHONY: test-ch004-final-schema-store"
    print "test-ch004-final-schema-store:"
    print "\t@bash scripts/test-ch004-final-schema-store.sh"
    print ""
    print ".PHONY: benchmark-ch004-final-schema-store"
    print "benchmark-ch004-final-schema-store:"
    print "\t@bash scripts/benchmark-ch004-final-schema-store.sh"
    print ""
    print ".PHONY: format-ch004-final-schema-store"
    print "format-ch004-final-schema-store:"
    print "\t@bash scripts/format-ch004-final-schema-store.sh"
    print ""
    print ".PHONY: test-ch004-final-schema-store-package"
    print "test-ch004-final-schema-store-package:"
    print "\t@bash scripts/test-ch004-final-schema-store-package.sh"
    print ""
    print ".PHONY: race-ch004-final-schema-store"
    print "race-ch004-final-schema-store:"
    print "\t@bash scripts/race-ch004-final-schema-store.sh"
    print ""
    print ".PHONY: vet-ch004-final-schema-store"
    print "vet-ch004-final-schema-store:"
    print "\t@bash scripts/vet-ch004-final-schema-store.sh"
    print ""
    print ".PHONY: deliver-ch004-final-schema-store"
    print "deliver-ch004-final-schema-store:"
    print "\t@bash scripts/deliver-ch004-final-schema-store.sh"
}' "$temporary_root/Makefile.base" > "$temporary_root/Makefile.feature"

set +e
diff -u --label a/Makefile --label b/Makefile "$temporary_root/Makefile.base" "$temporary_root/Makefile.feature" > "$temporary_root/Makefile.patch"
diff_status=$?
set -e
if [ "$diff_status" -gt 1 ]; then
	printf '%s\n' 'could not construct the Makefile feature patch' >&2
	exit 1
fi
git apply --cached --check "$temporary_root/Makefile.patch"
git apply --cached "$temporary_root/Makefile.patch"

git add -- \
  CH004_FINAL_SCHEMA_STORE.md \
  hat/hatSql/ch004_final_schema_store.go \
  hat/hatSql/ch004_final_schema_store_test.go \
  hat/hatSql/ch004_final_schema_store_benchmark_test.go \
  scripts/benchmark-ch004-final-schema-store.sh \
  scripts/deliver-ch004-final-schema-store.sh \
  scripts/format-ch004-final-schema-store.sh \
  scripts/race-ch004-final-schema-store.sh \
  scripts/test-ch004-final-schema-store-package.sh \
  scripts/test-ch004-final-schema-store.sh \
  scripts/vet-ch004-final-schema-store.sh

git diff --cached --check
git diff --cached --name-only > "$temporary_root/staged-paths"
while IFS= read -r path; do
	case "$path" in
		Makefile|CH004_FINAL_SCHEMA_STORE.md|hat/hatSql/ch004_final_schema_store.go|hat/hatSql/ch004_final_schema_store_test.go|hat/hatSql/ch004_final_schema_store_benchmark_test.go|scripts/benchmark-ch004-final-schema-store.sh|scripts/deliver-ch004-final-schema-store.sh|scripts/format-ch004-final-schema-store.sh|scripts/race-ch004-final-schema-store.sh|scripts/test-ch004-final-schema-store-package.sh|scripts/test-ch004-final-schema-store.sh|scripts/vet-ch004-final-schema-store.sh)
			;;
		*)
			printf 'refusing delivery: unexpected staged path %s\n' "$path" >&2
			exit 1
			;;
	esac
done < "$temporary_root/staged-paths"

git commit -m 'feat: persist SQL FINAL schema contracts [skip ci]'
git push origin HEAD

#!/usr/bin/env bash
set -euo pipefail

root_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
temporary_root=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-mz011-file-sink-deliver.XXXXXX")
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
{ print }
END {
    print ""
    print ".PHONY: benchmark-mz011-file-sink"
    print "benchmark-mz011-file-sink:"
    print "\t@bash scripts/benchmark-mz011-file-sink.sh"
    print ""
    print ".PHONY: format-mz011-file-sink"
    print "format-mz011-file-sink:"
    print "\t@bash scripts/format-mz011-file-sink.sh"
    print ""
    print ".PHONY: test-mz011-file-sink"
    print "test-mz011-file-sink:"
    print "\t@bash scripts/test-mz011-file-sink.sh"
    print ""
    print ".PHONY: test-mz011-file-sink-package"
    print "test-mz011-file-sink-package:"
    print "\t@bash scripts/test-mz011-file-sink-package.sh"
    print ""
    print ".PHONY: race-mz011-file-sink"
    print "race-mz011-file-sink:"
    print "\t@bash scripts/race-mz011-file-sink.sh"
    print ""
    print ".PHONY: vet-mz011-file-sink"
    print "vet-mz011-file-sink:"
    print "\t@bash scripts/vet-mz011-file-sink.sh"
    print ""
    print ".PHONY: deliver-mz011-file-sink"
    print "deliver-mz011-file-sink:"
    print "\t@bash scripts/deliver-mz011-file-sink.sh"
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
  MZ011_FILE_COMMAND_JOURNAL_SINK.md \
  hat/hatCache/mz011_file_sink.go \
  hat/hatCache/mz011_file_sink_test.go \
  hat/hatCache/mz011_file_sink_benchmark_test.go \
  scripts/benchmark-mz011-file-sink.sh \
  scripts/deliver-mz011-file-sink.sh \
  scripts/format-mz011-file-sink.sh \
  scripts/race-mz011-file-sink.sh \
  scripts/test-mz011-file-sink-package.sh \
  scripts/test-mz011-file-sink.sh \
  scripts/vet-mz011-file-sink.sh

git diff --cached --check
git diff --cached --name-only > "$temporary_root/staged-paths"
while IFS= read -r path; do
	case "$path" in
		Makefile|MZ011_FILE_COMMAND_JOURNAL_SINK.md|hat/hatCache/mz011_file_sink.go|hat/hatCache/mz011_file_sink_test.go|hat/hatCache/mz011_file_sink_benchmark_test.go|scripts/benchmark-mz011-file-sink.sh|scripts/deliver-mz011-file-sink.sh|scripts/format-mz011-file-sink.sh|scripts/race-mz011-file-sink.sh|scripts/test-mz011-file-sink-package.sh|scripts/test-mz011-file-sink.sh|scripts/vet-mz011-file-sink.sh)
			;;
		*)
			printf 'refusing delivery: unexpected staged path %s\n' "$path" >&2
			exit 1
			;;
	esac
done < "$temporary_root/staged-paths"

git commit -m 'feat: add durable command journal file sink [skip ci]'
git push origin HEAD

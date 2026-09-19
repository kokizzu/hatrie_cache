#!/usr/bin/env bash
set -euo pipefail

repo=$(git rev-parse --show-toplevel)
head=$(git rev-parse HEAD)
stage=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-chu39-commit.XXXXXX")
archive="$stage/archive.tar"
index="$stage/index"

cleanup() {
	rm -rf "$stage"
}
trap cleanup EXIT

git -C "$repo" archive --format=tar --output="$archive" "$head"
tar -xf "$archive" -C "$stage"
rm -f "$archive"

feature_files=(
	CHU39_WORKLOAD_ADMISSION.md
	hat/hatSql/sql_workload_admission.go
	hat/hatSql/chu39_workload_admission_test.go
	hat/hatSql/chu39_workload_admission_benchmark_test.go
	scripts/format-chu39.sh
	scripts/test-chu39.sh
	scripts/benchmark-chu39.sh
	scripts/test-chu39-package.sh
	scripts/race-chu39.sh
	scripts/vet-chu39.sh
	scripts/verify-chu39.sh
	scripts/commit-chu39.sh
	scripts/push-chu39.sh
)
for path in "${feature_files[@]}"; do
	if [[ ! -f "$repo/$path" ]]; then
		printf 'missing feature file: %s\n' "$path" >&2
		exit 1
	fi
	cp "$repo/$path" "$stage/$path"
done

perl -0pi -e '$_ .= qq{\n## Workload admission priorities\n\nSQL handlers can opt into bounded, priority-aware admission with cancellation,\nweighted class sharing, starvation protection, stats, and panic-safe permit\nrelease through `hatSql.SQLWorkloadAdmission`. Existing SQL execution remains\nunchanged when no controller is configured. See\n[CHU39_WORKLOAD_ADMISSION.md](CHU39_WORKLOAD_ADMISSION.md).\n};' "$stage/README.md"

perl -0pi -e 'my $old = q{| CH-U39 | Workload admission priorities | Query resource limits do not provide a scheduler with user-defined workload classes and starvation protection. | Fairness, cancellation, and default zero overhead. |}; my $new = q{| CH-U39 | Workload admission priorities | Implemented as opt-in `SQLWorkloadAdmission` with bounded pending work, priority and weighted class selection, starvation protection, cancellation, close, stats, and panic-safe release. See [CHU39_WORKLOAD_ADMISSION.md](CHU39_WORKLOAD_ADMISSION.md). | Fairness, cancellation, and default zero overhead. |}; die "missing CH-U39 row\n" unless s/\Q$old\E/$new/;' "$stage/PRODUCT_IDEA_GAPS.md"

perl -0pi -e '$_ .= qq{\n## CH-U39 Workload Admission Priorities\n\nThe controller is opt-in, so the default SQL path has no admission overhead.\nThese measurements use a 64-value integer-sum callback, five benchmark samples\nper case, and the AMD Ryzen 9 5950X test host.\n\n| Case | Raw samples (ns/op) | Median | Memory |\n| --- | --- | ---: | --- |\n| Direct callback | 22.94, 22.73, 22.06, 22.79, 21.96 | 22.73 | 0 B/op, 0 allocs/op |\n| Uncontended acquire/release | 24.36, 24.21, 24.66, 24.83, 24.81 | 24.66 | 0 B/op, 0 allocs/op |\n| `Run` | 55.01, 51.84, 50.89, 52.73, 54.18 | 52.73 | 0 B/op, 0 allocs/op |\n\nBefore the notification-path optimization, acquire measured `102.8 ns/op`,\n`112 B/op`, and one allocation at the median; `Run` measured `142.5 ns/op`,\n`112 B/op`, and one allocation. The final result removes that allocation and\nis approximately 4.2x faster for acquire and 2.7x faster for `Run`. The\ndeferred release remains to recover permits when a callback panics.\n};' "$stage/BENCHMARK.md"

perl -0pi -e '$_ .= qq{\n.PHONY: format-chu39 test-chu39 benchmark-chu39\nformat-chu39:\n\tbash ./scripts/format-chu39.sh\n\ntest-chu39:\n\tbash ./scripts/test-chu39.sh\n\nbenchmark-chu39:\n\tbash ./scripts/benchmark-chu39.sh\n\ntest-chu39-package:\n\tbash ./scripts/test-chu39-package.sh\n\nrace-chu39:\n\tbash ./scripts/race-chu39.sh\n\nvet-chu39:\n\tbash ./scripts/vet-chu39.sh\n\nverify-chu39:\n\tbash ./scripts/verify-chu39.sh\n\n.PHONY: commit-chu39 push-chu39\ncommit-chu39:\n\tbash ./scripts/commit-chu39.sh\n\npush-chu39:\n\tbash ./scripts/push-chu39.sh\n};' "$stage/Makefile"

run_git() {
	GIT_INDEX_FILE="$index" git -C "$repo" --work-tree="$stage" "$@"
}

run_git read-tree "$head"
run_git add -- Makefile README.md BENCHMARK.md PRODUCT_IDEA_GAPS.md "${feature_files[@]}"
run_git diff --cached --stat
tree=$(run_git write-tree)
commit=$(git -C "$repo" commit-tree "$tree" -p "$head" -m "feat: add opt-in SQL workload admission")
current=$(git -C "$repo" rev-parse HEAD)
if [[ "$current" != "$head" ]]; then
	printf 'HEAD changed during isolated commit: %s -> %s\n' "$head" "$current" >&2
	exit 1
fi
git -C "$repo" update-ref HEAD "$commit" "$head"
printf 'created commit %s\n' "$commit"

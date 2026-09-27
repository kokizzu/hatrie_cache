#!/usr/bin/env bash
set -euo pipefail

mode="${1:-status}"
repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

feature_files=(
  TT024_MATERIALIZED_TEXT_AUTO_SELECTION.md
  hat/hatSchema/text_index.go
  hat/hatSchema/text_index_resolver.go
  hat/hatSchema/tt024_text_auto_index_test.go
  hat/hatSchema/tt024_text_auto_index_benchmark_test.go
  hat/hatSql/index_advisor.go
  scripts/benchmark-tt024-materialized-text.sh
  scripts/format-tt024-materialized-text.sh
  scripts/race-tt024-materialized-text.sh
  scripts/test-tt024-materialized-text-packages.sh
  scripts/test-tt024-materialized-text.sh
  scripts/vet-tt024-materialized-text.sh
  scripts/deliver-tt024-materialized-text.sh
)

allowed_path() {
  case "$1" in
    TT024_MATERIALIZED_TEXT_AUTO_SELECTION.md|Makefile|BENCHMARK.md|INSPIRATION.md|hat/hatSchema/text_index.go|hat/hatSchema/text_index_resolver.go|hat/hatSchema/tt024_text_auto_index_test.go|hat/hatSchema/tt024_text_auto_index_benchmark_test.go|hat/hatSql/index_advisor.go|scripts/benchmark-tt024-materialized-text.sh|scripts/format-tt024-materialized-text.sh|scripts/race-tt024-materialized-text.sh|scripts/test-tt024-materialized-text-packages.sh|scripts/test-tt024-materialized-text.sh|scripts/vet-tt024-materialized-text.sh|scripts/deliver-tt024-materialized-text.sh)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

check_staged_scope() {
  local staged path
  staged="$(git diff --cached --name-only)"
  for path in $staged; do
    if ! allowed_path "$path"; then
      printf 'refusing to touch unrelated staged path: %s\n' "$path" >&2
      exit 1
    fi
  done
}

write_selective_patch() {
  local patch_file="$1"
  cat > "$patch_file" <<'PATCH'
diff --git a/Makefile b/Makefile
--- a/Makefile
+++ b/Makefile
@@ -28632,5 +28632,46 @@
 test-tt024-package:
 @@TAB@@bash ./scripts/test-tt024-package.sh
+.PHONY: test-tt024-materialized-text
+test-tt024-materialized-text:
+	bash scripts/test-tt024-materialized-text.sh
+
+.PHONY: benchmark-tt024-materialized-text
+benchmark-tt024-materialized-text:
+	bash scripts/benchmark-tt024-materialized-text.sh
+
+.PHONY: format-tt024-materialized-text
+format-tt024-materialized-text:
+	bash scripts/format-tt024-materialized-text.sh
+
+.PHONY: race-tt024-materialized-text test-tt024-materialized-text-packages vet-tt024-materialized-text
+race-tt024-materialized-text:
+	bash scripts/race-tt024-materialized-text.sh
+
+test-tt024-materialized-text-packages:
+	bash scripts/test-tt024-materialized-text-packages.sh
+
+vet-tt024-materialized-text:
+	bash scripts/vet-tt024-materialized-text.sh
+
+.PHONY: stage-tt024-materialized-text commit-tt024-materialized-text push-tt024-materialized-text deliver-tt024-materialized-text status-tt024-materialized-text
+stage-tt024-materialized-text:
+	bash scripts/deliver-tt024-materialized-text.sh stage
+
+commit-tt024-materialized-text:
+	bash scripts/deliver-tt024-materialized-text.sh commit
+
+push-tt024-materialized-text:
+	bash scripts/deliver-tt024-materialized-text.sh push
+
+deliver-tt024-materialized-text:
+	bash scripts/deliver-tt024-materialized-text.sh deliver
+
+status-tt024-materialized-text:
+	bash scripts/deliver-tt024-materialized-text.sh status
+
+.PHONY: unstage-tt024-materialized-text
+unstage-tt024-materialized-text:
+	bash scripts/deliver-tt024-materialized-text.sh unstage
 .PHONY: deliver-tt024-mixed-boolean
 deliver-tt024-mixed-boolean:
 @@TAB@@bash ./scripts/deliver-tt024-mixed-boolean.sh deliver
diff --git a/INSPIRATION.md b/INSPIRATION.md
--- a/INSPIRATION.md
+++ b/INSPIRATION.md
@@ -1060,4 +1060,10 @@
 - [x] T156 ClickHouse-style optional gzip compression for public command and
   batch request bodies. JSON remains the default; `Client.CommandCompressionThreshold`
   enables bandwidth reduction for larger JSON or protobuf requests.
+- [x] T157 Tarantool-style automatic selection of an existing materialized text
+  index for literal `CONTAINS(field, query)` predicates. The SQL adapter now
+  intersects the smallest token posting lists and preserves full predicate
+  rechecks and scan fallback; see
+  [TT024_MATERIALIZED_TEXT_AUTO_SELECTION.md](TT024_MATERIALIZED_TEXT_AUTO_SELECTION.md)
+  and [BENCHMARK.md](BENCHMARK.md#tt-024-text-index-auto-selection).
 - [x] CHG02 ClickHouse-style small-cardinality grouped-state lookup. The first
diff --git a/BENCHMARK.md b/BENCHMARK.md
--- a/BENCHMARK.md
+++ b/BENCHMARK.md
@@ -37441,1 +37441,37 @@
+<a id="tt-024-text-index-auto-selection"></a>
+## TT-024 Materialized Text Index Auto-Selection
+
+The existing materialized positional text index was not previously selected
+for ordinary SQL `CONTAINS(field, literal)` queries through
+`SQLResolverAdapter`. This benchmark compares that full-scan fallback with
+automatic token-posting selection over the same deterministic 20,000-row
+source. Five `-benchmem` samples ran on Linux/amd64 with an AMD Ryzen 9 5950X.
+
+| Path | Median ns/op | Median B/op | Median allocs/op | Relative result |
+| --- | ---: | ---: | ---: | --- |
+| Full scan (`scan_baseline`) | 25,548,346 | 21,950,523 | 180,034 | Baseline |
+| Automatic text index (`automatic_text_index`) | 37,894 | 21,608 | 173 | 674.2x lower CPU, 1,015.9x fewer bytes, 1,040.7x fewer allocations |
+
+The measured bytes are timed allocation volume, not retained index memory. The
+tradeoff is retained token/posting sidecar memory and index maintenance during
+builds/writes in exchange for substantially less read work. Full predicate
+rechecks and the old scan path remain in place for correctness and fallback.
+Implementation and test details are in
+[TT024_MATERIALIZED_TEXT_AUTO_SELECTION.md](TT024_MATERIALIZED_TEXT_AUTO_SELECTION.md).
+
+Raw output from `make benchmark-tt024-materialized-text`:
+
+```text
+BenchmarkTT024MaterializedTextIndexSelection/scan_baseline-32          45  26053179 ns/op  21950524 B/op  180034 allocs/op
+BenchmarkTT024MaterializedTextIndexSelection/scan_baseline-32          46  25355048 ns/op  21950523 B/op  180034 allocs/op
+BenchmarkTT024MaterializedTextIndexSelection/scan_baseline-32          46  25460686 ns/op  21950517 B/op  180034 allocs/op
+BenchmarkTT024MaterializedTextIndexSelection/scan_baseline-32          45  26164124 ns/op  21950637 B/op  180034 allocs/op
+BenchmarkTT024MaterializedTextIndexSelection/scan_baseline-32          44  25548346 ns/op  21950518 B/op  180034 allocs/op
+BenchmarkTT024MaterializedTextIndexSelection/automatic_text_index-32  33000     37979 ns/op     21608 B/op       173 allocs/op
+BenchmarkTT024MaterializedTextIndexSelection/automatic_text_index-32  31130     36341 ns/op     21608 B/op       173 allocs/op
+BenchmarkTT024MaterializedTextIndexSelection/automatic_text_index-32  33244     36510 ns/op     21608 B/op       173 allocs/op
+BenchmarkTT024MaterializedTextIndexSelection/automatic_text_index-32  30375     38082 ns/op     21608 B/op       173 allocs/op
+BenchmarkTT024MaterializedTextIndexSelection/automatic_text_index-32  31264     37894 ns/op     21608 B/op       173 allocs/op
+```
+
 <a id="tt-024-text-index-or-union"></a>
PATCH
  sed -i $'s/@@TAB@@/\t/g' "$patch_file"
}

cleanup_stage_files() {
  rm -f "$repo_root"/.tt024-materialized-text-stage.*
}

stage_feature() {
  local patch_file
  cleanup_stage_files
  check_staged_scope
  if ! git diff --cached --quiet; then
    printf 'refusing to stage with existing staged changes\n' >&2
    exit 1
  fi
  for path in "${feature_files[@]}"; do
    git add -- "$path"
  done
  patch_file="$(mktemp "$repo_root/.tt024-materialized-text-stage.XXXXXX")"
  write_selective_patch "$patch_file"
  git apply --cached --check "$patch_file"
  git apply --cached "$patch_file"
  rm -f "$patch_file"
  git diff --cached --check
  check_staged_scope
  git diff --cached --stat
  cleanup_stage_files
}

status_feature() {
  check_staged_scope
  git status --short -- "${feature_files[@]}" Makefile BENCHMARK.md INSPIRATION.md
  git diff --cached --stat -- "${feature_files[@]}" Makefile BENCHMARK.md INSPIRATION.md
}

unstage_feature() {
  local path
  check_staged_scope
  while IFS= read -r path; do
    [ -n "$path" ] || continue
    git restore --staged -- "$path"
  done < <(git diff --cached --name-only -- "${feature_files[@]}" Makefile BENCHMARK.md INSPIRATION.md)
  cleanup_stage_files
}

commit_feature() {
  if git diff --cached --quiet; then
    stage_feature
  else
    check_staged_scope
    git diff --cached --check
  fi
  if git diff --cached --quiet; then
    printf 'no staged TT-024 materialized-text changes to commit\n' >&2
    exit 1
  fi
  git commit -m 'feat(sql): auto-select materialized text indexes [skip ci]'
}

push_feature() {
  check_staged_scope
  git push
}

case "$mode" in
  status)
    status_feature
    ;;
  unstage)
    unstage_feature
    ;;
  stage)
    stage_feature
    ;;
  commit)
    commit_feature
    ;;
  push)
    push_feature
    ;;
  deliver)
    commit_feature
    push_feature
    ;;
  *)
    printf 'usage: %s {status|stage|commit|push|deliver}\n' "$0" >&2
    exit 2
    ;;
esac

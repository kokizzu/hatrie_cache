#!/usr/bin/env bash
set -euo pipefail

paths=(
  hat/hatDataStructure/tt045_tuple_compression.go
  hat/hatDataStructure/tt045_tuple_compression_test.go
  hat/hatDataStructure/tt045_tuple_compression_benchmark_test.go
  hat/hatDataStructure/tt045_tuple_compression_size_benchmark_test.go
  scripts/format-tt045-c309.sh
  scripts/test-tt045-c309.sh
  scripts/test-race-tt045-c309.sh
  scripts/vet-tt045-c309.sh
  scripts/benchmark-tt045-c309.sh
  scripts/review-tt045-c310.sh
  scripts/stage-tt045-c310.sh
  scripts/inspect-staged-tt045-c310.sh
  scripts/commit-tt045-c310.sh
  scripts/push-tt045-c310.sh
  TT045_TUPLE_COMPRESSION.md
  README.md
  ENGINE_IDEAS.md
  ADOPTED_QUERY_ENGINE_IDEAS.md
  BENCHMARK.md
)

mapfile -t preexisting_staged < <(git diff --cached --name-only)
if ((${#preexisting_staged[@]} != 0)); then
  printf '%s\n' 'Refusing to mix pre-existing staged paths:' >&2
  printf '  %s\n' "${preexisting_staged[@]}" >&2
  exit 1
fi

for path in "${paths[@]}"; do
  [[ -e "$path" ]] || { printf 'missing feature path: %s\n' "$path" >&2; exit 1; }
done
git add -- "${paths[@]}"

tmp=$(mktemp)
trap 'rm -f "$tmp"' EXIT
git show HEAD:Makefile > "$tmp"

if ! grep -q '^format-tt045-c309:' "$tmp"; then
  cat >> "$tmp" <<'MAKE'

.PHONY: format-tt045-c309 test-tt045-c309 test-race-tt045-c309 vet-tt045-c309 benchmark-tt045-c309

format-tt045-c309:
	bash ./scripts/format-tt045-c309.sh

test-tt045-c309:
	bash ./scripts/test-tt045-c309.sh

test-race-tt045-c309:
	bash ./scripts/test-race-tt045-c309.sh

vet-tt045-c309:
	bash ./scripts/vet-tt045-c309.sh

benchmark-tt045-c309:
	bash ./scripts/benchmark-tt045-c309.sh
MAKE
fi

if ! grep -q '^review-tt045-c310:' "$tmp"; then
  cat >> "$tmp" <<'MAKE'

.PHONY: review-tt045-c310 stage-tt045-c310 inspect-staged-tt045-c310 commit-tt045-c310 push-tt045-c310

review-tt045-c310:
	bash ./scripts/review-tt045-c310.sh

stage-tt045-c310:
	bash ./scripts/stage-tt045-c310.sh

inspect-staged-tt045-c310:
	bash ./scripts/inspect-staged-tt045-c310.sh

commit-tt045-c310:
	bash ./scripts/commit-tt045-c310.sh

push-tt045-c310:
	bash ./scripts/push-tt045-c310.sh
MAKE
fi

blob=$(git hash-object -w "$tmp")
git update-index --add --cacheinfo "100644,$blob,Makefile"
git diff --cached --check

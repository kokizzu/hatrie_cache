#!/usr/bin/env bash
set -euo pipefail

paths=(
  hat/hatPipeline/mutation_dependency_graph.go
  hat/hatPipeline/mutation_dependency_graph_test.go
  hat/hatPipeline/mutation_dependency_graph_performance_test.go
  scripts/format-ch014-c302.sh
  scripts/test-race-ch014-c302.sh
  scripts/vet-ch014-c302.sh
  scripts/benchmark-ch014-c302.sh
  scripts/review-ch014-c303.sh
  scripts/stage-ch014-c303.sh
  scripts/inspect-staged-ch014-c303.sh
  scripts/commit-ch014-c303.sh
  scripts/push-ch014-c303.sh
  MUTATION_DEPENDENCY_GRAPH.md
  README.md
  ENGINE_IDEAS.md
  ADOPTED_QUERY_ENGINE_IDEAS.md
  BENCHMARK.md
)

for path in "${paths[@]}"; do
  test -f "$path"
done

while IFS= read -r path; do
  case "$path" in
    Makefile|hat/hatPipeline/mutation_dependency_graph.go|hat/hatPipeline/mutation_dependency_graph_test.go|hat/hatPipeline/mutation_dependency_graph_performance_test.go|scripts/format-ch014-c302.sh|scripts/test-race-ch014-c302.sh|scripts/vet-ch014-c302.sh|scripts/benchmark-ch014-c302.sh|scripts/review-ch014-c303.sh|scripts/stage-ch014-c303.sh|scripts/inspect-staged-ch014-c303.sh|scripts/commit-ch014-c303.sh|scripts/push-ch014-c303.sh|MUTATION_DEPENDENCY_GRAPH.md|README.md|ENGINE_IDEAS.md|ADOPTED_QUERY_ENGINE_IDEAS.md|BENCHMARK.md) ;;
    *) printf 'unexpected pre-staged path: %s\n' "$path" >&2; exit 1 ;;
  esac
done < <(git diff --cached --name-only)

makefile=$(mktemp)
trap 'rm -f "$makefile"' EXIT
git show HEAD:Makefile > "$makefile"
cat >> "$makefile" <<'EOF'

.PHONY: test-ch014-c302
test-ch014-c302:
	@bash scripts/test-ch014-c302.sh

.PHONY: format-ch014-c302
format-ch014-c302:
	@bash scripts/format-ch014-c302.sh

.PHONY: test-race-ch014-c302
test-race-ch014-c302:
	@bash scripts/test-race-ch014-c302.sh

.PHONY: vet-ch014-c302
vet-ch014-c302:
	@bash scripts/vet-ch014-c302.sh

.PHONY: benchmark-ch014-c302
benchmark-ch014-c302:
	@bash scripts/benchmark-ch014-c302.sh

.PHONY: review-ch014-c303
review-ch014-c303:
	@bash scripts/review-ch014-c303.sh

.PHONY: stage-ch014-c303
stage-ch014-c303:
	@bash scripts/stage-ch014-c303.sh

.PHONY: inspect-staged-ch014-c303
inspect-staged-ch014-c303:
	@bash scripts/inspect-staged-ch014-c303.sh

.PHONY: commit-ch014-c303
commit-ch014-c303:
	@bash scripts/commit-ch014-c303.sh

.PHONY: push-ch014-c303
push-ch014-c303:
	@bash scripts/push-ch014-c303.sh
EOF
blob=$(git hash-object -w "$makefile")
git update-index --add --cacheinfo "100644,$blob,Makefile"
git add -- "${paths[@]}"

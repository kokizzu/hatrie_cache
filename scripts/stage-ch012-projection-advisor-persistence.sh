#!/usr/bin/env bash
set -euo pipefail

repo_root=$(git rev-parse --show-toplevel)
cd "$repo_root"

feature_files=(
  BENCHMARK.md
  ADOPTED_QUERY_ENGINE_IDEAS.md
  CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md
  CH012_PROJECTION_ADVISOR_PERSISTENCE.md
  hat/hatSql/ch012_projection_advisor_persistence.go
  hat/hatSql/ch012_projection_advisor_persistence_test.go
  hat/hatSql/ch012_projection_advisor_persistence_benchmark_test.go
  scripts/test-ch012-projection-advisor-persistence.sh
  scripts/benchmark-ch012-projection-advisor-persistence.sh
  scripts/format-ch012-projection-advisor-persistence.sh
  scripts/race-ch012-projection-advisor-persistence.sh
  scripts/vet-ch012-projection-advisor-persistence.sh
  scripts/measure-ch012-projection-advisor-persistence.sh
  scripts/stage-ch012-projection-advisor-persistence.sh
  scripts/commit-ch012-projection-advisor-persistence.sh
  scripts/push-ch012-projection-advisor-persistence.sh
)

for path in "${feature_files[@]}"; do
  if [[ ! -f "$path" ]]; then
    printf 'missing feature file: %s\n' "$path" >&2
    exit 1
  fi
done

# The worktree contains unrelated parallel Makefile edits. Build the staged
# Makefile from HEAD and append only this feature's command API.
temporary_makefile=$(mktemp)
trap 'rm -f "$temporary_makefile"' EXIT
git show HEAD:Makefile > "$temporary_makefile"
cat >> "$temporary_makefile" <<'EOF'

.PHONY: test-ch012-projection-advisor-persistence benchmark-ch012-projection-advisor-persistence format-ch012-projection-advisor-persistence race-ch012-projection-advisor-persistence vet-ch012-projection-advisor-persistence measure-ch012-projection-advisor-persistence stage-ch012-projection-advisor-persistence commit-ch012-projection-advisor-persistence push-ch012-projection-advisor-persistence
test-ch012-projection-advisor-persistence:
	bash ./scripts/test-ch012-projection-advisor-persistence.sh
benchmark-ch012-projection-advisor-persistence:
	bash ./scripts/benchmark-ch012-projection-advisor-persistence.sh
format-ch012-projection-advisor-persistence:
	bash ./scripts/format-ch012-projection-advisor-persistence.sh
race-ch012-projection-advisor-persistence:
	bash ./scripts/race-ch012-projection-advisor-persistence.sh
vet-ch012-projection-advisor-persistence:
	bash ./scripts/vet-ch012-projection-advisor-persistence.sh
measure-ch012-projection-advisor-persistence:
	bash ./scripts/measure-ch012-projection-advisor-persistence.sh
stage-ch012-projection-advisor-persistence:
	bash ./scripts/stage-ch012-projection-advisor-persistence.sh
commit-ch012-projection-advisor-persistence:
	bash ./scripts/commit-ch012-projection-advisor-persistence.sh
push-ch012-projection-advisor-persistence:
	bash ./scripts/push-ch012-projection-advisor-persistence.sh
EOF

git add -- "${feature_files[@]}"
makefile_blob=$(git hash-object -w "$temporary_makefile")
git update-index --add --cacheinfo "100644,$makefile_blob,Makefile"
printf 'staged CH012 projection-advisor persistence files:\n'
git diff --cached --name-only --

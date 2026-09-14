#!/usr/bin/env bash
set -euo pipefail

feature_paths=(
  ADOPTED_QUERY_ENGINE_IDEAS.md
  BENCHMARK.md
  CH013_MUTATION_ADMISSION.md
  INSPIRATION_BACKLOG.md
  README.md
  hat/hatCache/ch013_mutation_admission_benchmark_test.go
  hat/hatCache/ch013_mutation_admission_test.go
  hat/hatCache/sql.go
  hat/hatSql/ch013_mutation_admission_test.go
  hat/hatSql/query.go
  hat/hatSql/sql_mutation_admission.go
  scripts/benchmark-ch013-c203.sh
  scripts/benchmark-ch013-gate-c203.sh
  scripts/commit-ch013-c203.sh
  scripts/format-ch013-c203.sh
  scripts/inspect-staged-ch013-c203.sh
  scripts/push-ch013-c203.sh
  scripts/stage-ch013-c203.sh
  scripts/test-ch013-c203.sh
  scripts/verify-ch013-c203.sh
)

if ! git diff --cached --quiet; then
  printf '%s\n' 'refusing to mix an existing index with CH-013' >&2
  exit 1
fi

for path in "${feature_paths[@]}"; do
  if [[ ! -e "$path" ]]; then
    printf 'missing CH-013 path: %s\n' "$path" >&2
    exit 1
  fi
done

makefile_tmp=$(mktemp)
trap 'rm -f "$makefile_tmp"' EXIT
git show HEAD:Makefile >"$makefile_tmp"
if ! rg -q '^\.PHONY: test-ch013-c203$' "$makefile_tmp"; then
  cat >>"$makefile_tmp" <<'EOF'

.PHONY: test-ch013-c203
test-ch013-c203:
	bash ./scripts/test-ch013-c203.sh

.PHONY: benchmark-ch013-c203
benchmark-ch013-c203:
	bash ./scripts/benchmark-ch013-c203.sh

.PHONY: benchmark-ch013-gate-c203
benchmark-ch013-gate-c203:
	bash ./scripts/benchmark-ch013-gate-c203.sh

.PHONY: format-ch013-c203
format-ch013-c203:
	bash ./scripts/format-ch013-c203.sh

.PHONY: verify-ch013-c203
verify-ch013-c203:
	bash ./scripts/verify-ch013-c203.sh

.PHONY: stage-ch013-c203
stage-ch013-c203:
	bash ./scripts/stage-ch013-c203.sh

.PHONY: inspect-staged-ch013-c203
inspect-staged-ch013-c203:
	bash ./scripts/inspect-staged-ch013-c203.sh

.PHONY: commit-ch013-c203
commit-ch013-c203:
	bash ./scripts/commit-ch013-c203.sh

.PHONY: push-ch013-c203
push-ch013-c203:
	bash ./scripts/push-ch013-c203.sh
EOF
fi

makefile_blob=$(git hash-object -w "$makefile_tmp")
git update-index --add --cacheinfo "100644,$makefile_blob,Makefile"
git add -- "${feature_paths[@]}"

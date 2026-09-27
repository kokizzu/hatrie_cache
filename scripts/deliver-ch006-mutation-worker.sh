#!/usr/bin/env bash
set -euo pipefail

target_block=$(cat <<'EOF'

.PHONY: test-ch006-mutation-worker benchmark-ch006-mutation-worker format-ch006-mutation-worker verify-ch006-mutation-worker deliver-ch006-mutation-worker
test-ch006-mutation-worker:
	@bash scripts/test-ch006-mutation-worker.sh
benchmark-ch006-mutation-worker:
	@bash scripts/benchmark-ch006-mutation-worker.sh
format-ch006-mutation-worker:
	@bash scripts/format-ch006-mutation-worker.sh
verify-ch006-mutation-worker:
	@bash scripts/verify-ch006-mutation-worker.sh
deliver-ch006-mutation-worker:
	@bash scripts/deliver-ch006-mutation-worker.sh
EOF
)

staged_makefile="$(mktemp "${TMPDIR:-/tmp}/hatrie-ch006-worker-makefile.XXXXXX")"
trap 'rm -f "$staged_makefile"' EXIT
git show HEAD:Makefile > "$staged_makefile"
printf '%s\n' "$target_block" >> "$staged_makefile"
makefile_blob="$(git hash-object -w "$staged_makefile")"
git update-index --add --cacheinfo "100644,$makefile_blob,Makefile"

git add \
  CH006_MUTATION_WORKER.md \
  hat/hatSql/ch006_mutation_worker_baseline_test.go \
  hat/hatSql/ch006_mutation_worker_test.go \
  hat/hatSql/sql_mutation_dependency_queue_worker.go \
  scripts/benchmark-ch006-mutation-worker.sh \
  scripts/deliver-ch006-mutation-worker.sh \
  scripts/format-ch006-mutation-worker.sh \
  scripts/test-ch006-mutation-worker.sh \
  scripts/verify-ch006-mutation-worker.sh

git diff --cached --check
git commit -m 'feat: add durable mutation queue worker [skip ci]'
git push

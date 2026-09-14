#!/usr/bin/env bash
set -euo pipefail

git add -- \
  BENCHMARK.md \
  INSPIRATION_BACKLOG.md \
  scripts/stage-ch033-rejection-c203.sh \
  scripts/inspect-staged-ch033-rejection-c203.sh \
  scripts/commit-ch033-rejection-c203.sh \
  scripts/push-ch033-rejection-c203.sh

head_makefile=$(mktemp)
candidate_makefile=$(mktemp)
makefile_patch=$(mktemp)
trap 'rm -f "$head_makefile" "$candidate_makefile" "$makefile_patch"' EXIT

git show HEAD:Makefile > "$head_makefile"
cp "$head_makefile" "$candidate_makefile"
cat >> "$candidate_makefile" <<'EOF'

.PHONY: stage-ch033-rejection-c203
stage-ch033-rejection-c203:
	bash ./scripts/stage-ch033-rejection-c203.sh

.PHONY: inspect-staged-ch033-rejection-c203
inspect-staged-ch033-rejection-c203:
	bash ./scripts/inspect-staged-ch033-rejection-c203.sh

.PHONY: commit-ch033-rejection-c203
commit-ch033-rejection-c203:
	bash ./scripts/commit-ch033-rejection-c203.sh

.PHONY: push-ch033-rejection-c203
push-ch033-rejection-c203:
	bash ./scripts/push-ch033-rejection-c203.sh
EOF

set +e
diff -u --label a/Makefile --label b/Makefile "$head_makefile" "$candidate_makefile" > "$makefile_patch"
diff_status=$?
set -e
if [[ "$diff_status" -eq 1 ]]; then
  git apply --cached "$makefile_patch"
elif [[ "$diff_status" -ne 0 ]]; then
  exit "$diff_status"
fi

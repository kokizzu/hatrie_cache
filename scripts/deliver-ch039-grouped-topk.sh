#!/usr/bin/env bash
set -euo pipefail

expected_branch='codex/inspiration-next-10'
commit_message='feat: add CH-039 grouped approximate top-k fast path [skip ci]'
required_paths=(
  'CH039_GROUPED_APPROXIMATE_AGGREGATES.md'
  'CH039_GROUPED_TOPK.md'
  'ENGINE_IDEAS.md'
  'hat/hatSql/approx_aggregate.go'
  'hat/hatSql/m052c_native_dataflow.go'
  'hat/hatSql/ch039_grouped_topk_native_test.go'
  'hat/hatSql/ch039_grouped_topk_benchmark_test.go'
  'scripts/format-ch039-grouped-topk.sh'
  'scripts/test-ch039-grouped-topk.sh'
  'scripts/test-ch039-approx-regression.sh'
  'scripts/race-ch039-grouped-topk.sh'
  'scripts/test-ch039-package.sh'
  'scripts/vet-ch039-package.sh'
  'scripts/benchmark-ch039-grouped-topk.sh'
  'scripts/deliver-ch039-grouped-topk.sh'
)

branch=$(git branch --show-current)
if [[ "$branch" != "$expected_branch" ]]; then
	echo "refusing delivery on branch $branch; expected $expected_branch" >&2
	exit 1
fi
if ! git diff --cached --quiet; then
	echo 'refusing delivery with pre-existing staged changes' >&2
	exit 1
fi
for path in "${required_paths[@]}"; do
	if [[ ! -e "$path" ]]; then
		echo "missing required path: $path" >&2
		exit 1
	fi
done

tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-ch039-delivery.XXXXXX")
trap 'rm -rf "$tmp_dir"' EXIT

tmp_makefile="$tmp_dir/Makefile"
git show HEAD:Makefile > "$tmp_makefile"
if grep -Fq 'deliver-ch039-grouped-topk' "$tmp_makefile"; then
	echo 'CH-039 delivery target already exists in committed Makefile' >&2
	exit 1
fi
printf '\n.PHONY: deliver-ch039-grouped-topk\ndeliver-ch039-grouped-topk:\n\tbash ./scripts/deliver-ch039-grouped-topk.sh\n' >> "$tmp_makefile"

git add -- \
  CH039_GROUPED_APPROXIMATE_AGGREGATES.md \
  CH039_GROUPED_TOPK.md \
  ENGINE_IDEAS.md \
  hat/hatSql/approx_aggregate.go \
  hat/hatSql/m052c_native_dataflow.go \
  hat/hatSql/ch039_grouped_topk_native_test.go \
  hat/hatSql/ch039_grouped_topk_benchmark_test.go \
  scripts/format-ch039-grouped-topk.sh \
  scripts/test-ch039-grouped-topk.sh \
  scripts/test-ch039-approx-regression.sh \
  scripts/race-ch039-grouped-topk.sh \
  scripts/test-ch039-package.sh \
  scripts/vet-ch039-package.sh \
  scripts/benchmark-ch039-grouped-topk.sh \
  scripts/deliver-ch039-grouped-topk.sh

makefile_blob=$(git hash-object -w "$tmp_makefile")
git update-index --add --cacheinfo "100644,$makefile_blob,Makefile"
git diff --cached --check
printf '%s\n' 'Staged CH-039 paths:'
git diff --cached --name-only
git commit -m "$commit_message"
git push origin HEAD

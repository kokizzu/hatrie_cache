#!/usr/bin/env bash
set -euo pipefail

repo=$(cd "$(dirname "$0")/.." && pwd)
cd "$repo"

action=${1:-}
files=(
	Makefile
	README.md
	BENCHMARK.md
	MU038_PERSISTED_IMMUTABLE_PARTS.md
	PRODUCT_IDEA_GAPS.md
	hat/hatStorage/immutable_part_manifest.go
	hat/hatStorage/mu38_immutable_part_manifest_test.go
	hat/hatStorage/mu38_immutable_part_manifest_benchmark_test.go
	scripts/format-mu38.sh
	scripts/test-mu38.sh
	scripts/benchmark-mu38.sh
	scripts/race-mu38.sh
	scripts/vet-mu38.sh
	scripts/verify-mu38.sh
	scripts/deliver-mu38.sh
)

case "$action" in
stage)
	git add -- "${files[@]}"
	git diff --cached --check
	git status --short -- "${files[@]}"
	;;
commit)
	git diff --cached --check
	if git diff --cached --quiet; then
		printf '%s\n' 'no staged M-U38 changes'
		exit 0
	fi
	git commit -m 'feat(storage): persist immutable part manifests [skip ci]'
	;;
push)
	git push -u origin codex/mu38-immutable-parts
	;;
*)
	printf 'usage: %s {stage|commit|push}\n' "$0" >&2
	exit 2
	;;
esac

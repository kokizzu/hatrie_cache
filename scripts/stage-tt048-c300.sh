#!/usr/bin/env bash
set -euo pipefail

files=(
	ADOPTED_QUERY_ENGINE_IDEAS.md
	BENCHMARK.md
	DATA_STRUCTURE.md
	ENGINE_IDEAS.md
	Makefile
	PRIORITY_VISIBILITY_QUEUE.md
	README.md
	VISIBILITY_QUEUE.md
	hat/hatDataStructure/priority_visibility_queue.go
	hat/hatDataStructure/priority_visibility_queue_test.go
	scripts/benchmark-tt048-c296.sh
	scripts/format-tt048-c296.sh
	scripts/test-race-tt048-c296.sh
	scripts/vet-tt048-c296.sh
	scripts/review-tt048-c300.sh
	scripts/stage-tt048-c300.sh
	scripts/inspect-staged-tt048-c300.sh
	scripts/commit-tt048-c300.sh
	scripts/push-tt048-c300.sh
)

staged_before="$(git diff --cached --name-only)"
if [[ -n "$staged_before" ]]; then
	while IFS= read -r path; do
		[[ -z "$path" ]] && continue
		allowed=false
		for candidate in "${files[@]}"; do
			if [[ "$path" == "$candidate" ]]; then
				allowed=true
				break
			fi
		done
		if [[ "$allowed" != true ]]; then
			printf 'refusing to mix pre-staged path: %s\n' "$path" >&2
			exit 1
		fi
	done <<< "$staged_before"
fi

git add -- \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	BENCHMARK.md \
	DATA_STRUCTURE.md \
	ENGINE_IDEAS.md \
	PRIORITY_VISIBILITY_QUEUE.md \
	README.md \
	VISIBILITY_QUEUE.md \
	hat/hatDataStructure/priority_visibility_queue.go \
	hat/hatDataStructure/priority_visibility_queue_test.go \
	scripts/benchmark-tt048-c296.sh \
	scripts/format-tt048-c296.sh \
	scripts/test-race-tt048-c296.sh \
	scripts/vet-tt048-c296.sh \
	scripts/review-tt048-c300.sh \
	scripts/stage-tt048-c300.sh \
	scripts/inspect-staged-tt048-c300.sh \
	scripts/commit-tt048-c300.sh \
	scripts/push-tt048-c300.sh

base_file="$(mktemp)"
desired_file="$(mktemp)"
trap 'rm -f "$base_file" "$desired_file"' EXIT
git show HEAD:Makefile > "$base_file"
cp "$base_file" "$desired_file"
printf '\n# TT-048 priority visibility queue targets\n' >> "$desired_file"
printf '%s\n' \
	'.PHONY: format-tt048-c296' \
	'format-tt048-c296:' \
	$'\t@bash scripts/format-tt048-c296.sh' \
	'.PHONY: test-race-tt048-c296' \
	'test-race-tt048-c296:' \
	$'\t@bash scripts/test-race-tt048-c296.sh' \
	'.PHONY: benchmark-tt048-c296' \
	'benchmark-tt048-c296:' \
	$'\t@bash scripts/benchmark-tt048-c296.sh' \
	'.PHONY: vet-tt048-c296' \
	'vet-tt048-c296:' \
	$'\t@bash scripts/vet-tt048-c296.sh' \
	'.PHONY: review-tt048-c300' \
	'review-tt048-c300:' \
	$'\t@bash scripts/review-tt048-c300.sh' \
	'.PHONY: stage-tt048-c300' \
	'stage-tt048-c300:' \
	$'\t@bash scripts/stage-tt048-c300.sh' \
	'.PHONY: inspect-staged-tt048-c300' \
	'inspect-staged-tt048-c300:' \
	$'\t@bash scripts/inspect-staged-tt048-c300.sh' \
	'.PHONY: commit-tt048-c300' \
	'commit-tt048-c300:' \
	$'\t@bash scripts/commit-tt048-c300.sh' \
	'.PHONY: push-tt048-c300' \
	'push-tt048-c300:' \
	$'\t@bash scripts/push-tt048-c300.sh' >> "$desired_file"
makefile_blob="$(git hash-object -w "$desired_file")"
git update-index --add --cacheinfo "100644,$makefile_blob,Makefile"

git diff --cached --check
git diff --cached --name-only

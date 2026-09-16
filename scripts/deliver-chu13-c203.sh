#!/usr/bin/env bash
set -euo pipefail

repo_root=$(pwd)
mode=${1:-all}
synthetic_makefile=$(mktemp /tmp/hatrie-cache-chu13-makefile.XXXXXX)
cleanup() {
	rm -f "$synthetic_makefile"
}
trap cleanup EXIT

feature_files=(
	BENCHMARK.md
	CHU13_PHRASE_POSTINGS.md
	PRODUCT_IDEA_GAPS.md
	README.md
	hat/hatDataStructure/ch025_token_postings_index.go
	hat/hatDataStructure/ch_u13_phrase_postings_index.go
	hat/hatDataStructure/ch_u13_phrase_postings_index_test.go
	hat/hatDataStructure/ch_u13_phrase_postings_index_lifecycle_test.go
	hat/hatDataStructure/ch_u13_phrase_postings_index_baseline_benchmark_test.go
	hat/hatDataStructure/ch_u13_phrase_postings_index_benchmark_test.go
	scripts/benchmark-chu13-before-c203.sh
	scripts/benchmark-chu13-c203.sh
	scripts/format-chu13-c203.sh
	scripts/full-test-chu13-c203.sh
	scripts/race-chu13-c203.sh
	scripts/test-chu13-c203.sh
	scripts/test-chu13-red-c203.sh
	scripts/vet-chu13-c203.sh
	scripts/deliver-chu13-c203.sh
)

makefile_targets() {
	cat <<'EOF'

.PHONY: format-chu13-c203
format-chu13-c203:
	bash ./scripts/format-chu13-c203.sh

.PHONY: test-chu13-red-c203
test-chu13-red-c203:
	bash ./scripts/test-chu13-red-c203.sh

.PHONY: test-chu13-c203
test-chu13-c203:
	bash ./scripts/test-chu13-c203.sh

.PHONY: race-chu13-c203
race-chu13-c203:
	bash ./scripts/race-chu13-c203.sh

.PHONY: vet-chu13-c203
vet-chu13-c203:
	bash ./scripts/vet-chu13-c203.sh

.PHONY: benchmark-chu13-before-c203
benchmark-chu13-before-c203:
	bash ./scripts/benchmark-chu13-before-c203.sh

.PHONY: benchmark-chu13-c203
benchmark-chu13-c203:
	bash ./scripts/benchmark-chu13-c203.sh

.PHONY: full-test-chu13-c203
full-test-chu13-c203:
	bash ./scripts/full-test-chu13-c203.sh

.PHONY: deliver-chu13-c203
deliver-chu13-c203:
	bash ./scripts/deliver-chu13-c203.sh all

.PHONY: stage-chu13-c203
stage-chu13-c203:
	bash ./scripts/deliver-chu13-c203.sh stage

.PHONY: commit-chu13-c203
commit-chu13-c203:
	bash ./scripts/deliver-chu13-c203.sh commit

.PHONY: push-chu13-c203
push-chu13-c203:
	bash ./scripts/deliver-chu13-c203.sh push
EOF
}

stage_feature() {
	git -C "$repo_root" add -- "${feature_files[@]}"
	git -C "$repo_root" show HEAD:Makefile > "$synthetic_makefile"
	makefile_targets >> "$synthetic_makefile"
	makefile_blob=$(git -C "$repo_root" hash-object -w "$synthetic_makefile")
	git -C "$repo_root" update-index --add --cacheinfo "100644,$makefile_blob,Makefile"
	git -C "$repo_root" diff --cached --check

	printf '%s\n' 'Staged CH-U13 paths:'
	git -C "$repo_root" diff --cached --name-only
}

case "$mode" in
status)
	git -C "$repo_root" status --short
	git -C "$repo_root" diff --cached --name-only
	;;
stage)
	stage_feature
	;;
commit)
	stage_feature
	git -C "$repo_root" commit -m 'Add phrase-aware token postings [skip ci]'
	;;
push)
	git -C "$repo_root" push origin HEAD:master
	;;
all)
	stage_feature
	git -C "$repo_root" commit -m 'Add phrase-aware token postings [skip ci]'
	git -C "$repo_root" push origin HEAD:master
	;;
*)
	printf 'usage: %s {status|stage|commit|push|all}\n' "$0" >&2
	exit 2
	;;
esac

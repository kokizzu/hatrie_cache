#!/usr/bin/env bash
set -euo pipefail

mode=${1:-preview}
repo_root=$(git rev-parse --show-toplevel)
cd "$repo_root"

tmpdir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-pipeline-delivery.XXXXXX")
trap 'rm -rf -- "$tmpdir"' EXIT
index="$tmpdir/index"

if [[ "$mode" == "push" ]]; then
	git push
	git rev-parse HEAD
	exit 0
fi

if [[ "$mode" != "preview" && "$mode" != "commit" ]]; then
	echo "usage: $0 [preview|commit|push]" >&2
	exit 2
fi

target_block=$(cat <<'EOF'
# pipeline-stage-targets
format-pipeline-stages:
	bash ./scripts/format-pipeline-stages.sh

test-pipeline-stages:
	bash ./scripts/test-pipeline-stages.sh

test-race-pipeline-stages:
	bash ./scripts/test-race-pipeline-stages.sh

benchmark-pipeline-stages:
	bash ./scripts/benchmark-pipeline-stages.sh

deliver-pipeline-stages:
	bash ./scripts/deliver-pipeline-stages.sh preview

commit-pipeline-stages:
	bash ./scripts/deliver-pipeline-stages.sh commit

push-pipeline-stages:
	bash ./scripts/deliver-pipeline-stages.sh push
EOF
)
base_makefile="$tmpdir/Makefile"
git show HEAD:Makefile > "$base_makefile"
if ! grep -Fq -- '# pipeline-stage-targets' "$base_makefile"; then
	printf '\n%s\n' "$target_block" >> "$base_makefile"
fi

base_inspiration="$tmpdir/INSPIRATION.md"
git show HEAD:INSPIRATION.md > "$base_inspiration"
child='- [x] C016a Bounded independently scheduled pipeline stages with backpressure and cancellation.'
if ! grep -Fq -- "$child" "$base_inspiration"; then
	printf '\n%s\n' "$child" >> "$base_inspiration"
fi

makefile_blob=$(git hash-object -w "$base_makefile")
inspiration_blob=$(git hash-object -w "$base_inspiration")
GIT_INDEX_FILE="$index" git read-tree HEAD
GIT_INDEX_FILE="$index" git update-index --add --cacheinfo "100644,$makefile_blob,Makefile"
GIT_INDEX_FILE="$index" git update-index --add --cacheinfo "100644,$inspiration_blob,INSPIRATION.md"
GIT_INDEX_FILE="$index" git add -- \
	PIPELINE_STAGES.md \
	hat/hatPipeline/pipeline.go \
	hat/hatPipeline/pipeline_test.go \
	scripts/benchmark-pipeline-stages.sh \
	scripts/deliver-pipeline-stages.sh \
	scripts/format-pipeline-stages.sh \
	scripts/test-pipeline-stages.sh \
	scripts/test-race-pipeline-stages.sh

GIT_INDEX_FILE="$index" git diff --cached --check
if GIT_INDEX_FILE="$index" git diff --cached --quiet; then
	echo "pipeline stages already delivered"
	git rev-parse HEAD
	exit 0
fi

echo "pipeline stages delivery ($mode)"
GIT_INDEX_FILE="$index" git diff --cached --name-status
GIT_INDEX_FILE="$index" git diff --cached --stat

if [[ "$mode" == "commit" ]]; then
	GIT_INDEX_FILE="$index" git commit -m "feat(pipeline): add independently scheduled stages"
	git rev-parse HEAD
fi

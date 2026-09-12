#!/usr/bin/env bash
set -euo pipefail

root=$(pwd)
files=(
	ASYNC_BATCHER.md
	hat/hatPipeline/async_batcher.go
	hat/hatPipeline/async_batcher_test.go
	scripts/benchmark-t-async-batcher.sh
	scripts/format-t-async-batcher.sh
	scripts/race-t-async-batcher.sh
	scripts/test-t-async-batcher-package.sh
	scripts/test-t-async-batcher.sh
	scripts/vet-t-async-batcher.sh
	scripts/commit-t-async-batcher.sh
	scripts/publish-t-async-batcher.sh
)

git fetch origin master
base=$(git rev-parse origin/master)
worktree=$(mktemp -d /tmp/hatrie-cache-t-async-batcher.XXXXXX)
cleanup() {
	git -C "$root" worktree remove --force "$worktree" >/dev/null 2>&1 || true
}
trap cleanup EXIT

git -C "$root" worktree add --detach "$worktree" "$base"
for file in "${files[@]}"; do
	destination="$worktree/$file"
	mkdir -p "$(dirname "$destination")"
	cp "$root/$file" "$destination"
done

if ! rg -q 'ASYNC_BATCHER.md' "$worktree/README.md"; then
	awk '
		{ print }
		!inserted && $0 == "## Start Here" {
			print "- ClickHouse-style opt-in asynchronous batch ingestion: [ASYNC_BATCHER.md](ASYNC_BATCHER.md)"
			inserted = 1
		}
		END {
			if (!inserted) {
				print "- ClickHouse-style opt-in asynchronous batch ingestion: [ASYNC_BATCHER.md](ASYNC_BATCHER.md)"
			}
		}
	' "$worktree/README.md" > "$worktree/README.md.async"
	mv "$worktree/README.md.async" "$worktree/README.md"
fi

if ! rg -q '^test-t-async-batcher:' "$worktree/Makefile"; then
	printf '\n.PHONY: test-t-async-batcher\ntest-t-async-batcher:\n\tbash ./scripts/test-t-async-batcher.sh\n\n.PHONY: format-t-async-batcher\nformat-t-async-batcher:\n\tbash ./scripts/format-t-async-batcher.sh\n\n.PHONY: test-t-async-batcher-package\ntest-t-async-batcher-package:\n\tbash ./scripts/test-t-async-batcher-package.sh\n\n.PHONY: race-t-async-batcher\nrace-t-async-batcher:\n\tbash ./scripts/race-t-async-batcher.sh\n\n.PHONY: vet-t-async-batcher\nvet-t-async-batcher:\n\tbash ./scripts/vet-t-async-batcher.sh\n\n.PHONY: benchmark-t-async-batcher\nbenchmark-t-async-batcher:\n\tbash ./scripts/benchmark-t-async-batcher.sh\n\n.PHONY: commit-t-async-batcher\ncommit-t-async-batcher:\n\tbash ./scripts/commit-t-async-batcher.sh\n\n.PHONY: publish-t-async-batcher\npublish-t-async-batcher:\n\tbash ./scripts/publish-t-async-batcher.sh\n' >> "$worktree/Makefile"
fi

git -C "$worktree" add Makefile README.md "${files[@]}"
git -C "$worktree" diff --cached --check
git -C "$worktree" commit -m 'feat(pipeline): add bounded async batcher'
go test -C "$worktree" ./... -timeout 90s -p 1 -skip '^TestRunRestoreRehearsalVerifiesBackupPath$' -count=1
git -C "$worktree" push origin HEAD:master
git -C "$worktree" rev-parse HEAD

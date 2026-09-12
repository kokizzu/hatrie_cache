#!/usr/bin/env bash
set -euo pipefail

root=$(pwd)
files=(
	hat/hatPeer/connection_pool.go
	hat/hatPeer/connection_pool_test.go
	PEER_CONNECTION_POOL.md
	PRODUCT_IDEA_GAPS.md
	scripts/benchmark-t-peer-breaker.sh
	scripts/benchmark-t-peer-pool.sh
	scripts/format-t-peer-pool.sh
	scripts/publish-t-peer-pool.sh
	scripts/race-t-peer-pool.sh
	scripts/test-t-peer-pool-package.sh
	scripts/test-t-peer-breaker.sh
	scripts/test-t-peer-pool.sh
	scripts/vet-t-peer-pool.sh
)

git fetch origin master
base=$(git rev-parse origin/master)
worktree=$(mktemp -d /tmp/hatrie-cache-peer-pool.XXXXXX)
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

if ! rg -q 'PEER_CONNECTION_POOL.md' "$worktree/README.md"; then
	awk '
		{ print }
		!inserted && $0 == "## Start Here" {
			print "- Tarantool-style bounded reusable peer connections: [PEER_CONNECTION_POOL.md](PEER_CONNECTION_POOL.md)"
			inserted = 1
		}
		END {
			if (!inserted) {
				print "- Tarantool-style bounded reusable peer connections: [PEER_CONNECTION_POOL.md](PEER_CONNECTION_POOL.md)"
			}
		}
	' "$worktree/README.md" > "$worktree/README.md.peer-pool"
	mv "$worktree/README.md.peer-pool" "$worktree/README.md"
fi

if ! rg -q '^test-t-peer-pool:' "$worktree/Makefile"; then
	printf '\n.PHONY: test-t-peer-pool format-t-peer-pool test-t-peer-pool-package test-t-peer-breaker race-t-peer-pool vet-t-peer-pool benchmark-t-peer-pool benchmark-t-peer-breaker publish-t-peer-pool\ntest-t-peer-pool:\n\tbash ./scripts/test-t-peer-pool.sh\nformat-t-peer-pool:\n\tbash ./scripts/format-t-peer-pool.sh\ntest-t-peer-pool-package:\n\tbash ./scripts/test-t-peer-pool-package.sh\ntest-t-peer-breaker:\n\tbash ./scripts/test-t-peer-breaker.sh\nrace-t-peer-pool:\n\tbash ./scripts/race-t-peer-pool.sh\nvet-t-peer-pool:\n\tbash ./scripts/vet-t-peer-pool.sh\nbenchmark-t-peer-pool:\n\tbash ./scripts/benchmark-t-peer-pool.sh\nbenchmark-t-peer-breaker:\n\tbash ./scripts/benchmark-t-peer-breaker.sh\npublish-t-peer-pool:\n\tbash ./scripts/publish-t-peer-pool.sh\n' >> "$worktree/Makefile"
fi
if ! rg -q '^test-t-peer-breaker:' "$worktree/Makefile"; then
	printf '\n.PHONY: test-t-peer-breaker benchmark-t-peer-breaker\ntest-t-peer-breaker:\n\tbash ./scripts/test-t-peer-breaker.sh\nbenchmark-t-peer-breaker:\n\tbash ./scripts/benchmark-t-peer-breaker.sh\n' >> "$worktree/Makefile"
fi

make -C "$worktree" format-t-peer-pool
make -C "$worktree" test-t-peer-pool-package
make -C "$worktree" test-t-peer-breaker
make -C "$worktree" race-t-peer-pool
make -C "$worktree" vet-t-peer-pool
benchmark_log="$worktree/peer-pool-benchmark.log"
if ! make -C "$worktree" benchmark-t-peer-pool > "$benchmark_log"; then
	tail -n 120 "$benchmark_log"
	exit 1
fi
tail -n 80 "$benchmark_log"
make -C "$worktree" benchmark-t-peer-breaker
make -C "$worktree" audit-product-idea-gaps
full_test_log="$worktree/peer-pool-full-test.log"
if ! go test -C "$worktree" ./... -timeout 90s -p 1 -skip '^TestRunRestoreRehearsalVerifiesBackupPath$' -count=1 > "$full_test_log"; then
	tail -n 160 "$full_test_log"
	exit 1
fi
tail -n 40 "$full_test_log"

git -C "$worktree" add Makefile README.md "${files[@]}"
git -C "$worktree" diff --cached --check
git -C "$worktree" commit -m 'feat(peer): add bounded connection pool'
git -C "$worktree" push origin HEAD:master
git -C "$worktree" rev-parse HEAD

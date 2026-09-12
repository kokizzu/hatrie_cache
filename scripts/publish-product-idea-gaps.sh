#!/usr/bin/env bash
set -euo pipefail

root=$(pwd)
files=(
	PRODUCT_IDEA_GAPS.md
	scripts/audit-product-idea-gaps.sh
	scripts/commit-product-idea-gaps.sh
	scripts/publish-product-idea-gaps.sh
)

git fetch origin master
base=$(git rev-parse origin/master)
worktree=$(mktemp -d /tmp/hatrie-cache-product-idea-gaps.XXXXXX)
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

if ! rg -q 'PRODUCT_IDEA_GAPS.md' "$worktree/README.md"; then
	awk '
		{ print }
		!inserted && $0 == "## Start Here" {
			print "- Current 50-per-product implementation queue: [PRODUCT_IDEA_GAPS.md](PRODUCT_IDEA_GAPS.md)"
			inserted = 1
		}
		END {
			if (!inserted) {
				print "- Current 50-per-product implementation queue: [PRODUCT_IDEA_GAPS.md](PRODUCT_IDEA_GAPS.md)"
			}
		}
	' "$worktree/README.md" > "$worktree/README.md.product-ideas"
	mv "$worktree/README.md.product-ideas" "$worktree/README.md"
fi

if ! rg -q '^audit-product-idea-gaps:' "$worktree/Makefile"; then
	printf '\n.PHONY: audit-product-idea-gaps\naudit-product-idea-gaps:\n\tbash ./scripts/audit-product-idea-gaps.sh\n' >> "$worktree/Makefile"
fi

make -C "$worktree" audit-product-idea-gaps
git -C "$worktree" add Makefile README.md "${files[@]}"
git -C "$worktree" diff --cached --check
git -C "$worktree" commit -m 'docs: catalog unimplemented product ideas'
git -C "$worktree" push origin HEAD:master
git -C "$worktree" rev-parse HEAD

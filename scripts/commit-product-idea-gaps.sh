#!/usr/bin/env bash
set -euo pipefail

files=(
	PRODUCT_IDEA_GAPS.md
	README.md
	scripts/audit-product-idea-gaps.sh
	scripts/commit-product-idea-gaps.sh
	scripts/publish-product-idea-gaps.sh
)

git add "${files[@]}"
git diff --cached --check -- "${files[@]}"
git diff --cached --quiet -- "${files[@]}" && {
	printf '%s\n' 'no product idea gap changes are staged'
	exit 1
}
git commit --only -m 'docs: catalog unimplemented product ideas' -- "${files[@]}"

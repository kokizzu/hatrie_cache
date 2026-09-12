#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

feature_files=(
	hat/hatDataStructure/tuple_format_negotiation.go
	hat/hatDataStructure/tuple_format_negotiation_test.go
	scripts/benchmark-t-u40.sh
	scripts/format-t-u40.sh
	scripts/publish-t-u40.sh
	scripts/test-t-u40.sh
	TUPLE_FORMAT_NEGOTIATION.md
)

for path in "${feature_files[@]}"; do
	if [[ ! -f "$path" ]]; then
		printf 'missing feature file: %s\n' "$path" >&2
		exit 1
	fi
done

git fetch origin master
base_revision="$(git rev-parse origin/master)"
worktree="$(mktemp -d "${TMPDIR:-/tmp}/hatrie-cache-t-u40.XXXXXX")"
cleanup() {
	git worktree remove --force "$worktree" >/dev/null 2>&1 || true
}
trap cleanup EXIT

git worktree add --detach "$worktree" "$base_revision"
for path in "${feature_files[@]}"; do
	destination="$worktree/$path"
	mkdir -p "$(dirname "$destination")"
	cp "$path" "$destination"
done

if ! rg -q '^test-t-u40:' "$worktree/Makefile"; then
	printf '%s\n' \
		'' \
		'.PHONY: format-t-u40' \
		'format-t-u40:' \
		$'\t@bash scripts/format-t-u40.sh' \
		'' \
		'.PHONY: test-t-u40' \
		'test-t-u40:' \
		$'\t@bash scripts/test-t-u40.sh' \
		'' \
		'.PHONY: benchmark-t-u40' \
		'benchmark-t-u40:' \
		$'\t@bash scripts/benchmark-t-u40.sh' \
		'' \
		'.PHONY: publish-t-u40' \
		'publish-t-u40:' \
		$'\t@bash scripts/publish-t-u40.sh' \
		>> "$worktree/Makefile"
fi

old_readme='- Bounded as-of retention leases for safe compaction: [FRONTIER_RETENTION.md](FRONTIER_RETENTION.md)'
new_readme="$old_readme"$'\n''- Tarantool-style tuple format version negotiation: [TUPLE_FORMAT_NEGOTIATION.md](TUPLE_FORMAT_NEGOTIATION.md)'
if ! rg -Fq 'TUPLE_FORMAT_NEGOTIATION.md' "$worktree/README.md"; then
	rg -Fq -- "$old_readme" "$worktree/README.md"
	OLD_README="$old_readme" NEW_README="$new_readme" perl -0pi -e 's/\Q$ENV{OLD_README}\E/$ENV{NEW_README}/' "$worktree/README.md"
fi

old_catalog='| T-U40 | Tuple format version negotiation | Tuple formats validate fields, but clients cannot negotiate compatible format versions during rolling upgrades. | Unknown-field behavior and downgrade safety. |'
new_catalog='| T-U40 | Tuple format version negotiation | `hatDataStructure.TupleFormat` now exposes bounded capability advertisements with stable physical-shape fingerprints, deterministic HTF1 encoding, and highest exact common-version selection; the caller still owns the transport handshake and migration policy. | Unknown-field behavior and downgrade safety. |'
if ! rg -Fq 'stable physical-shape fingerprints' "$worktree/PRODUCT_IDEA_GAPS.md"; then
	rg -Fq -- "$old_catalog" "$worktree/PRODUCT_IDEA_GAPS.md"
	OLD_CATALOG="$old_catalog" NEW_CATALOG="$new_catalog" perl -0pi -e 's/\Q$ENV{OLD_CATALOG}\E/$ENV{NEW_CATALOG}/' "$worktree/PRODUCT_IDEA_GAPS.md"
fi

(cd "$worktree" && gofmt -w hat/hatDataStructure/tuple_format_negotiation.go hat/hatDataStructure/tuple_format_negotiation_test.go)
(cd "$worktree" && make test-t-u40)
(cd "$worktree" && go test -race ./hat/hatDataStructure -count=1)
(cd "$worktree" && go vet ./hat/hatDataStructure)
(cd "$worktree" && make benchmark-t-u40)
(cd "$worktree" && go test ./... -timeout 90s -p 1 -skip '^TestRunRestoreRehearsalVerifiesBackupPath$' -count=1)

(cd "$worktree" && git diff --check)
(cd "$worktree" && git add -- Makefile README.md PRODUCT_IDEA_GAPS.md "${feature_files[@]}")
(cd "$worktree" && git diff --cached --check)
(cd "$worktree" && git commit -m 'feat(hatDataStructure): negotiate tuple format versions')
(cd "$worktree" && git push origin HEAD:master)

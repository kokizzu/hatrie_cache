#!/usr/bin/env bash
set -euo pipefail

for path in CHU07_MUTATION_LIFECYCLE.md README.md BENCHMARK.md PRODUCT_IDEA_GAPS.md ADOPTED_QUERY_ENGINE_IDEAS.md; do
	if [[ ! -f "$path" ]]; then
		printf 'missing documentation file: %s\n' "$path" >&2
		exit 1
	fi
done

rg -n 'CHU07_MUTATION_LIFECYCLE\.md' README.md >/dev/null
rg -n '^## CH-U07 Mutation Lifecycle$' BENCHMARK.md >/dev/null
rg -n '\| CH-U07 \|' PRODUCT_IDEA_GAPS.md >/dev/null
rg -n 'CommandJournalSubmission' ADOPTED_QUERY_ENGINE_IDEAS.md >/dev/null
rg -n 'MutationStatus' CHU07_MUTATION_LIFECYCLE.md >/dev/null

#!/usr/bin/env bash
set -euo pipefail

for file in README.md PRODUCT_IDEA_GAPS.md BENCHMARK.md MU021_ROLE_NAMESPACE_CATALOG.md; do
	[[ -s "$file" ]]
done

rg -q 'MU021_ROLE_NAMESPACE_CATALOG.md' README.md
rg -q 'M-U21.*Role and namespace hierarchy.*RoleCatalog' PRODUCT_IDEA_GAPS.md
rg -q 'mu-021-role-and-namespace-catalog' BENCHMARK.md
rg -q 'BenchmarkMU021AfterRoleCatalogAuthorize' BENCHMARK.md
rg -q 'ErrRoleCatalogConflict|RoleCatalogSnapshot' MU021_ROLE_NAMESPACE_CATALOG.md

printf '%s\n' 'M-U21 documentation verified.'

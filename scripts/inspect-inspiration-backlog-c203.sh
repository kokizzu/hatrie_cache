#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' '== open inspiration backlog =='
rg -n '^\| (CH|MZ|TR)-.*\[ \]' INSPIRATION_BACKLOG.md || true
printf '%s\n' '== backlog counts =='
for prefix in CH MZ TT TR; do
    total=$(rg -c "^\\| ${prefix}-" INSPIRATION_BACKLOG.md || true)
    open=$(rg -c "^\\| ${prefix}-.*\\[ \\]" INSPIRATION_BACKLOG.md || true)
    printf '%s total=%s open=%s\n' "$prefix" "$total" "$open"
done
printf '%s\n' '== adopted catalog tail =='
rg -n '^\| (ClickHouse|Materialize|Tarantool)' ADOPTED_QUERY_ENGINE_IDEAS.md | tail -n 20 || true
printf '%s\n' '== related session targets =='
rg -n '^([a-z0-9-]*(ch|mz|tr|tt)[a-z0-9-]*):' Makefile | tail -n 40 || true
printf '%s\n' '== UUID and ordered-index surfaces =='
rg -n 'UUID|uuid|OrderedIndex|IndexSelectivity|Selectivity|Cursor|Resume' hat/hatSql hat/hatDataStructure hat/hatCache hat/hatTrie 2>/dev/null | head -n 160 || true
printf '%s\n' '== typed SQL and RowBinary surfaces =='
rg --files --sort path hat/hatSql -g '*uuid*' -g '*type*' -g '*row*binary*' -g '*ipv*' -g '*enum*' -g '*decimal*'
rg -n 'SQL(Decimal|IPv|Enum)|RowBinary|TypeName|Parse.*(IPv|Decimal|Enum)' hat/hatSql | head -n 220 || true
printf '%s\n' '== worktree =='
git status --short

#!/bin/sh
set -eu

test -f SQL_QUOTAS.md
rg -q '^## CH-029 User/key SQL quotas$' BENCHMARK.md
rg -q 'SQL_QUOTAS\.md' README.md
rg -q '^\| CH-029 \| User/key quotas \| Implemented' ENGINE_IDEAS.md
rg -q 'make benchmark-ch029-sql-quotas' SQL_QUOTAS.md BENCHMARK.md
rg -q 'ExecuteSQLQueryRows' SQL_QUOTAS.md
printf '%s\n' 'CH-029 documentation verified'

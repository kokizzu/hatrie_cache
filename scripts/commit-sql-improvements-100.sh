#!/usr/bin/env sh
set -eu

git diff --check -- SQL_IMPROVEMENTS_100.md scripts/audit-sql-improvements.sh scripts/inspect-sql-source-ownership.sh scripts/review-sql-improvements-100.sh scripts/commit-sql-improvements-100.sh Makefile
git add -- SQL_IMPROVEMENTS_100.md scripts/audit-sql-improvements.sh scripts/inspect-sql-source-ownership.sh scripts/review-sql-improvements-100.sh scripts/commit-sql-improvements-100.sh Makefile
git commit --only -m 'docs: add measured SQL improvement backlog' -- SQL_IMPROVEMENTS_100.md scripts/audit-sql-improvements.sh scripts/inspect-sql-source-ownership.sh scripts/review-sql-improvements-100.sh scripts/commit-sql-improvements-100.sh Makefile
git push origin master

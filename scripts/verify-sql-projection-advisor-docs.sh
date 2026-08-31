#!/bin/sh
set -eu

rg -q 'QueryID' PROJECTION_ADVISOR.md
rg -q 'benchmark-sql-projection-advisor' PROJECTION_ADVISOR.md
rg -q '\[SQL projection advisor\]\(PROJECTION_ADVISOR.md\)' README.md

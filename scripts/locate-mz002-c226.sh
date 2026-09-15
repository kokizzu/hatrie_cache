#!/usr/bin/env bash
set -euo pipefail

rg -n -C 2 'MZ-02|Public since|read hold' --glob '*.md'
printf '%s\n' '' 'ENGINE_IDEAS.md:'
sed -n '88,102p' ENGINE_IDEAS.md
printf '%s\n' '' 'INSPIRATION_BACKLOG.md:'
sed -n '90,98p' INSPIRATION_BACKLOG.md
printf '%s\n' '' 'ADOPTED_QUERY_ENGINE_IDEAS.md header:'
sed -n '1,70p' ADOPTED_QUERY_ENGINE_IDEAS.md

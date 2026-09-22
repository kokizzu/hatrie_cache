#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' '== branch/status =='
git status --short --branch
printf '%s\n' '== recent commits =='
git log -8 --oneline
printf '%s\n' '== inspiration and adoption documents =='
rg --files -g '*INSPIRATION*' -g '*IDEA*' -g '*BENCHMARK*' -g '*REPLICA*' | sort
printf '%s\n' '== round-2 checklist headings =='
if [[ -f INSPIRATION_ROUND2.md ]]; then
  rg -n '^(#|##|###)|T20[0-9]|T21[0-9]' INSPIRATION_ROUND2.md || true
fi
printf '%s\n' '== adopted idea entries =='
if [[ -f ADOPTED_QUERY_ENGINE_IDEAS.md ]]; then
  rg -n '^(#|##|###)|T20[0-9]|T21[0-9]' ADOPTED_QUERY_ENGINE_IDEAS.md || true
fi
printf '%s\n' '== replication package files =='
rg --files hat/hatReplication | sort
printf '%s\n' '== README replication references =='
rg -n 'T20[0-9]|replica|replication|conflict' README.md | head -n 100 || true
printf '%s\n' '== inspect-goal-state target declarations =='
rg -n -B 2 -A 2 '^inspect-goal-state' Makefile || true

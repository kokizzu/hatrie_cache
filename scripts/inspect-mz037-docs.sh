#!/bin/sh
set -eu

printf '%s\n' '--- README references ---'
rg -n -C 2 'WORKER_SCOPE|MZ-37|worker-local|exchange' README.md
printf '%s\n' '--- inspiration backlog ---'
sed -n '104,120p' INSPIRATION_BACKLOG.md
printf '%s\n' '--- benchmark tail ---'
tail -n 80 BENCHMARK.md

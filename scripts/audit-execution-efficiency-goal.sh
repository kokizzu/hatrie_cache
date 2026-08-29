#!/usr/bin/env sh
set -eu

printf '%s\n' '=== execution and storage files ==='
grep -R -l -E 'Columnar|columnar|vector|arena|dictionary|adaptive|zero.?copy|parallel|merge|join|lock|warm' hat/hatSql hat/hatCache hat/hatDataStructure --include='*.go' || true

printf '%s\n' '=== executor and storage markers ==='
grep -R -n -E 'Columnar|columnar|vector|arena|dictionary|adaptive|zero.?copy|parallel|merge|join|lock|warm' hat/hatSql hat/hatCache hat/hatDataStructure --include='*.go' || true

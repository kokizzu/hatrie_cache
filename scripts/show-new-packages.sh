#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$root"

printf '%s\n' '== recent package-oriented commits =='
git log --oneline -40 --all -- 'hat/**'
printf '%s\n' '== current public hat packages =='
go list ./hat/...

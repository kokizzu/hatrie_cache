#!/usr/bin/env bash
set -euo pipefail

git diff --check
git diff --cached --check
git status --short
printf '%s\n' '-- staged stat --'
git diff --cached --stat
printf '%s\n' '-- staged Makefile diff --'
git diff --cached -- Makefile
printf '%s\n' '-- staged protobuf generated diff summary --'
git diff --cached --numstat -- internal/gen/hatriecache/v1/cache.pb.go
git diff --cached -- internal/gen/hatriecache/v1/cache.pb.go | rg '^[+-].*(Code|code|CommandResponse)' || true
git diff --cached -- internal/gen/hatriecache/v1/cache.pb.go | sed -n '1,130p' || true
printf '%s\n' '-- HEAD Makefile tail --'
git show HEAD:Makefile | tail -n 20
printf '%s\n' '-- HEAD Makefile test area --'
git show HEAD:Makefile | nl -ba | sed -n '238,262p'
printf '%s\n' '-- staging script --'
nl -ba scripts/stage-t147.sh | sed -n '1,90p'

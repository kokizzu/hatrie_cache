#!/bin/sh
set -eu

git diff --check
printf '%s\n' 'Changed paths:'
git status --short
printf '%s\n' 'Diff stat:'
git diff --stat
printf '%s\n' 'Generated protobuf diff sample:'
git diff --numstat -- internal/gen/hatriecache/v1/cache.pb.go
git diff --unified=2 -- internal/gen/hatriecache/v1/cache.pb.go | head -n 180

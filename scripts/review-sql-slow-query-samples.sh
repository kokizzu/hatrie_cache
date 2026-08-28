#!/usr/bin/env sh
set -eu

git diff --check
git diff --name-only
git status --short

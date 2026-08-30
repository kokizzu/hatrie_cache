#!/usr/bin/env sh
set -eu

git status --short
git diff --check

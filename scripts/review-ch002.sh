#!/usr/bin/env bash
set -euo pipefail

git status --short
git diff --check
git diff --stat
git diff --name-only
rg -n 'inspect-ch002|inspect-adopted' Makefile || true

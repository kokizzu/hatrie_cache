#!/usr/bin/env bash
set -euo pipefail

git diff --check
git diff --stat
git diff -- hat/hatCache/main.go README.md ENGINE_IDEAS.md Makefile
git status --short

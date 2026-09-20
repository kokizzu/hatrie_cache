#!/usr/bin/env bash
set -euo pipefail

git diff --check
git status --short --branch
git diff --stat

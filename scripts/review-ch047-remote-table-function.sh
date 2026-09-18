#!/usr/bin/env bash
set -eu

git diff --check
git diff --cached --check
git status --short
git diff --stat
git diff --cached --stat

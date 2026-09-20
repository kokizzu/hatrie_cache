#!/usr/bin/env bash
set -eu

git diff --check
git status --short
git diff --stat

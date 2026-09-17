#!/bin/sh
set -eu

git diff --cached --check
git diff --cached --stat
git status --short

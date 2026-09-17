#!/bin/sh
set -eu

git status --short
git diff --stat
git ls-files --others --exclude-standard

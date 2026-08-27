#!/bin/sh
set -eu

git status --short
git diff --check
git diff --stat

#!/bin/sh
set -eu

git diff --check
git diff --stat
git status --short

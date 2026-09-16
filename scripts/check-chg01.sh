#!/bin/sh
set -eu
git diff --check
git diff --cached --check
git status --short
git diff --stat
git diff --cached --stat

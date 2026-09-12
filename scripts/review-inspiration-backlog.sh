#!/bin/sh
set -eu

git diff --check
git status --short

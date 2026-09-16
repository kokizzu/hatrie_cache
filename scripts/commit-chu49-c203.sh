#!/bin/sh
set -eu

git diff --cached --check
git commit -m "[skip ci] Add skip-index explain diagnostics"

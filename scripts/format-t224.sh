#!/usr/bin/env bash
set -euo pipefail

git diff --check
printf '%s\n' 'T224 formatting check passed.'

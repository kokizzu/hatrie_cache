#!/usr/bin/env bash
set -euo pipefail

git diff --check
printf '%s\n' 'T225 formatting check passed.'

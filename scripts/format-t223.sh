#!/usr/bin/env bash
set -euo pipefail

git diff --check
printf '%s\n' 'T223 formatting check passed.'

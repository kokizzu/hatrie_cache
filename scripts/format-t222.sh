#!/usr/bin/env bash
set -euo pipefail

git diff --check
printf '%s\n' 'T222 formatting check passed.'

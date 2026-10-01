#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
git commit -m 'feat(cache): add replica read-only enforcement [skip ci]'

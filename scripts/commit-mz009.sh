#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
git commit -m "Add timestamp domain leases [skip ci]"

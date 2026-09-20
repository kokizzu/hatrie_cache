#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
git commit -m "feat(data): add vertical TTL delete scan"

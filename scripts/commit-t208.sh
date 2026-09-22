#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
git commit -m "replication: add anonymous replica roles"

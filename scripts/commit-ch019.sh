#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
git commit -m "hatReplication: add replica part repair planning"

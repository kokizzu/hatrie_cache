#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
git commit -m "Optimize ordered snapshot cursor seeks"

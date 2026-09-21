#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
git commit -m "feat: fold differential subscription multiplicity"

#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
git commit -m "hatAudit: add opt-in metadata redaction"

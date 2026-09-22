#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' 'Open inspiration checklist items:'
grep -n "\\- \\[ \\]" INSPIRATION.md || true

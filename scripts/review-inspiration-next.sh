#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' 'Unchecked inspiration candidates:'
rg -n '^\| (CH|MZ|TR)-.*\| \[ \]' INSPIRATION_BACKLOG.md

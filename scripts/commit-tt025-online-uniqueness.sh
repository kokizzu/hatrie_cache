#!/usr/bin/env bash
set -euo pipefail

bash scripts/stage-tt025-online-uniqueness.sh
git commit -m 'build: include TT-025 Makefile targets'

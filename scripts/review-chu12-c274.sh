#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' '===== status ====='
git status --short
printf '%s\n' '===== diff stat ====='
git diff --stat
printf '%s\n' '===== Makefile diff ====='
git diff -- Makefile

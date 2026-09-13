#!/usr/bin/env bash
set -euo pipefail

git diff --check
git status --short
git diff --stat
rg -n '^(test|build|verify-all|test-ch050|race-ch050|vet-ch050|check-ch050|commit-ch050-rowbinary-import|push-ch050-rowbinary-import):' Makefile || true

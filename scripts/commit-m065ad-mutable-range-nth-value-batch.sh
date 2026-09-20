#!/usr/bin/env bash
set -eu

git diff --cached --check
git commit -m 'perf: batch same-position mutable range nth value'

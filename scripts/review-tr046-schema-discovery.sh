#!/usr/bin/env bash
set -eu
git diff --check
git diff --cached --check
git diff --stat
git diff --cached --stat

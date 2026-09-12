#!/usr/bin/env bash
set -euo pipefail

go test -run '^TestC212' ./hat/hatHash ./hat/hatSql

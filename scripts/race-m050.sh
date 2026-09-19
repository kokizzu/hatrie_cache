#!/usr/bin/env bash
set -euo pipefail

go test -race -run '^TestM050' ./hat/hatBackup

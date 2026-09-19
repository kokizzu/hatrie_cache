#!/usr/bin/env bash
set -euo pipefail

go test -run '^TestM050' ./hat/hatBackup

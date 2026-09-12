#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatBackup/chain.go hat/hatBackup/chain_test.go

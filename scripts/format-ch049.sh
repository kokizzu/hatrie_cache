#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatBackup/chain.go hat/hatBackup/encryption.go hat/hatBackup/model.go hat/hatBackup/object_store.go hat/hatBackup/encryption_test.go

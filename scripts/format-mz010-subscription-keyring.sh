#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
    hat/hatSql/mz010_subscription_keyring.go \
    hat/hatSql/mz010_subscription_keyring_benchmark_test.go \
    hat/hatSql/mz010_subscription_keyring_test.go

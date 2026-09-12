#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatPeer/retry_policy.go hat/hatPeer/retry_policy_test.go

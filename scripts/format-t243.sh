#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatPeer/tls_rotation.go hat/hatPeer/tls_rotation_test.go

#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatPgWire/server.go ./hat/hatPgWire/cancel.go ./hat/hatPgWire/metrics.go ./hat/hatPgWire/frontend_buffer_reuse_test.go -run '^TestReadFrontendMessageInto' -count=1

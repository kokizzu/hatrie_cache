#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPgWire/server.go ./hat/hatPgWire/cancel.go ./hat/hatPgWire/metrics.go ./hat/hatPgWire/backend_message_buffer_reuse_test.go ./hat/hatPgWire/backend_row_encoding_reuse_test.go -run '^TestReusableMessageConnection' -count=1

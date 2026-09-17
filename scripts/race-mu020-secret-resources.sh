#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatAuth -run '^(TestResourceRegistry|TestTokenSet|TestIdentity|TestPolicy)' -count=1

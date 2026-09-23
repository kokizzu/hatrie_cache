#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPeer -run '^$' -bench '^BenchmarkT241(AuthorizationBaseline|RoleAuthorizer)$' -benchmem -count=5

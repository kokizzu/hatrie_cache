#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run '^TestMZ010SubscriptionWireKeyring' -count=1

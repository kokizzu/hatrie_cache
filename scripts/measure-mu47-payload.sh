#!/usr/bin/env bash
set -euo pipefail

go test -tags=mu47 ./hat/hatSql -run '^TestQuerySubscriptionProgressFramePayloadSize$' -count=1 -v

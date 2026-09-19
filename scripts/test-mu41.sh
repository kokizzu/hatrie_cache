#!/usr/bin/env bash
set -euo pipefail

go test -tags mu41 ./hat/hatSql -run 'TestMU41WebhookEventDeduplicator' -count=1

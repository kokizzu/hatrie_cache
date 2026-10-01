#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatReplication/space_changefeed.go hat/hatReplication/space_changefeed_test.go hat/hatReplication/space_changefeed_example_test.go

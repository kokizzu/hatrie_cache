#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache ./hat/hatStorage -run '^TestPebblePropertiesExposeFilterAndReadAmplificationMetrics$' -count=1

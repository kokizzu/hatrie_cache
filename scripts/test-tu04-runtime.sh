#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatRuntime -run 'TestTU04'

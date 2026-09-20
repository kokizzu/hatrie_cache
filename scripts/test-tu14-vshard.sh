#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatTopology -run 'TestTU14'

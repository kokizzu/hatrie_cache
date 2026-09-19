#!/usr/bin/env bash
set -euo pipefail

go test -race -run '^TestMU048ConnectorHealth' ./hat/hatPipeline

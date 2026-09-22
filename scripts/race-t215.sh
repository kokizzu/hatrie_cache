#!/usr/bin/env bash
set -euo pipefail

go test -race -tags=t215 ./hat/hatDataStructure

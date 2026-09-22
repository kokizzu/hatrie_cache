#!/usr/bin/env bash
set -euo pipefail

go test -race -tags=t218 ./hat/hatDataStructure

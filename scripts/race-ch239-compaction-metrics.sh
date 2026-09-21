#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatStorage -run '^TestC239' -count=1

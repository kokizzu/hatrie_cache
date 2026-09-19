#!/usr/bin/env bash
set -euo pipefail

go test -race -run '^TestT048' ./hat/hatReplication

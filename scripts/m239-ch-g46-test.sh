#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDictionary -run '^TestCH046NegativeCache'

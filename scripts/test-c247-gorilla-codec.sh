#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run '^TestC247Gorilla' -count=1

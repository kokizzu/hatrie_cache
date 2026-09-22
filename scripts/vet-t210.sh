#!/usr/bin/env bash
set -euo pipefail

go vet -tags t210 ./hat/hatReplication

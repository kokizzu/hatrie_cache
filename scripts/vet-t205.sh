#!/usr/bin/env bash
set -euo pipefail

go vet ./hat/hatReplication ./hat/hatCache

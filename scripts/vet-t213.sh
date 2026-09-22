#!/usr/bin/env bash
set -euo pipefail

go vet -tags=t213 ./hat/hatCache

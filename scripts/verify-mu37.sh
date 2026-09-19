#!/usr/bin/env bash
set -euo pipefail

go test -tags mu37 ./hat/hatStorage
go test -race -tags mu37 ./hat/hatStorage
go vet -tags mu37 ./hat/hatStorage

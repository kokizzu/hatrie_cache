#!/usr/bin/env bash
set -euo pipefail

export GOCACHE="${GOCACHE:-/tmp/hatrie-tu19-go-cache}"
go test ./hat/hatDataStructure -run 'TestTupleFieldUpdateJournal'

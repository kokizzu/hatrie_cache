#!/usr/bin/env bash
set -euo pipefail

export GOCACHE=/tmp/hatrie-tu19-go-cache
go test -race ./hat/hatDataStructure -run 'TestTupleFieldUpdateJournal'
go vet ./hat/hatDataStructure

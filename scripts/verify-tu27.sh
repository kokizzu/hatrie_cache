#!/usr/bin/env bash
set -euo pipefail

export GOCACHE=/tmp/hatrie-tu26-go-cache
go test -race ./hat/hatPeer -run 'TestPeerConfigWatcher'
go vet ./hat/hatPeer

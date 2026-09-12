#!/bin/sh
set -eu

go test ./hat/hatPeer -count=1
go test ./hat/hatPeer -race -run '^TestCompactRequestTemplate' -count=1
go vet ./hat/hatPeer

#!/bin/sh
set -eu

go test ./hat/hatPeer -race -run '^TestCompactPeerSessionPropagatesRequestCancellation$' -count=1

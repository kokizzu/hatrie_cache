#!/bin/sh
set -eu

go test ./hat/hatPeer -run '^TestCompactPeerSessionPropagatesRequestCancellation$' -count=1

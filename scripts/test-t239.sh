#!/bin/sh
set -eu

go test ./hat/hatPeer -run '^TestCompactRequestTemplate' -count=1

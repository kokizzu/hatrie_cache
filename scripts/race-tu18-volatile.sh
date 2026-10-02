#!/bin/sh
set -eu

GOTOOLCHAIN=auto GOCACHE=/tmp/hatrie-cache-round58-gocache go test -race ./hat/hatCache -run '^TestVolatileHatTrie' -count=1

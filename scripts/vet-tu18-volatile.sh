#!/bin/sh
set -eu

GOTOOLCHAIN=auto GOCACHE=/tmp/hatrie-cache-round58-gocache go vet ./hat/hatCache

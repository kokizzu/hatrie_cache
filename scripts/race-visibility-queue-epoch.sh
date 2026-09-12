#!/bin/sh
set -eu

exec go test -race -run '^TestVisibilityQueue(Epoch|Token)' ./hat/hatDataStructure

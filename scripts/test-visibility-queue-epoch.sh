#!/bin/sh
set -eu

exec go test -run '^TestVisibilityQueue(Epoch|Token)' ./hat/hatDataStructure

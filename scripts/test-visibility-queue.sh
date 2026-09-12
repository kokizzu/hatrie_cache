#!/bin/sh
set -eu
exec go test -run '^TestVisibilityQueue' ./hat/hatDataStructure

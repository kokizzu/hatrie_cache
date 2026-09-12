#!/bin/sh
set -eu

exec go test -race -run '^TestExecuteWriteQuorumUntilSatisfied' ./hat/hatReplication

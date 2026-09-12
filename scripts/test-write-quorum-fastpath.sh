#!/bin/sh
set -eu

exec go test -run '^TestExecuteWriteQuorumUntilSatisfied' ./hat/hatReplication

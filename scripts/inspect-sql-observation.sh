#!/bin/sh
set -eu

rg -n -C 3 'type SQLQueryObserver|type SQLQueryEvent|func \(observation sqlQueryObservation\) finish|SQLSlowQueryRecorder' hat/hatSql

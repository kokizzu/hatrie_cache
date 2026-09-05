package hatriecache

import core "hatrie_cache/hat/hatCache"

type SQLJSONIndexRebuildProgress = core.SQLJSONIndexRebuildProgress
type SQLJSONIndexRebuildState = core.SQLJSONIndexRebuildState

const (
	SQLJSONIndexRebuildStateQueued    = core.SQLJSONIndexRebuildStateQueued
	SQLJSONIndexRebuildStateRunning   = core.SQLJSONIndexRebuildStateRunning
	SQLJSONIndexRebuildStateCompleted = core.SQLJSONIndexRebuildStateCompleted
	SQLJSONIndexRebuildStateFailed    = core.SQLJSONIndexRebuildStateFailed
	SQLJSONIndexRebuildStateCanceled  = core.SQLJSONIndexRebuildStateCanceled
)

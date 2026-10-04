// Package hatSort provides bounded, stable external sorting primitives.
//
// ExternalSort is an opt-in building block for callers that need to sort more
// records than their memory budget can hold. It spills sorted runs to a caller
// selected directory, merges them deterministically, and removes every
// temporary run before returning.
package hatSort

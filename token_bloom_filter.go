package hatriecache

import "hatrie_cache/hat/hatDataStructure"

// TokenBloomFilter is the public root-package alias for the reusable token
// Bloom prefilter.
type TokenBloomFilter = hatDataStructure.TokenBloomFilter

// TokenBloomFilterSnapshot is the compact token Bloom snapshot format.
type TokenBloomFilterSnapshot = hatDataStructure.TokenBloomFilterSnapshot

var NewTokenBloomFilter = hatDataStructure.NewTokenBloomFilter
var NewTokenBloomFilterWithShape = hatDataStructure.NewTokenBloomFilterWithShape
var NewTokenBloomFilterFromSnapshot = hatDataStructure.NewTokenBloomFilterFromSnapshot
var ValidateTokenBloomFilterSnapshot = hatDataStructure.ValidateTokenBloomFilterSnapshot

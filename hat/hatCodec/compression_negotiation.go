package hatCodec

import (
	"compress/flate"
	"errors"
)

var ErrCompressionLevelPolicyInvalid = errors.New("hatriecache: compression level policy is invalid")

// CompressionLevelPolicy contains the server-supported range and preferred
// level for a compressed-block client.
type CompressionLevelPolicy struct {
	defaultLevel int
	minLevel     int
	maxLevel     int
}

// NewCompressionLevelPolicy validates a server compression level range.
// flate.NoCompression is excluded because CompressedBlockOptions uses zero as
// its omitted-level value and normalizes it to flate.BestSpeed.
func NewCompressionLevelPolicy(defaultLevel, minLevel, maxLevel int) (CompressionLevelPolicy, error) {
	if !validNegotiatedCompressionLevel(defaultLevel) || !validNegotiatedCompressionLevel(minLevel) ||
		!validNegotiatedCompressionLevel(maxLevel) || minLevel > maxLevel || defaultLevel < minLevel || defaultLevel > maxLevel {
		return CompressionLevelPolicy{}, ErrCompressionLevelPolicyInvalid
	}
	return CompressionLevelPolicy{defaultLevel: defaultLevel, minLevel: minLevel, maxLevel: maxLevel}, nil
}

// DefaultCompressionLevelPolicy returns the current compressed-block defaults.
func DefaultCompressionLevelPolicy() CompressionLevelPolicy {
	return CompressionLevelPolicy{defaultLevel: flate.BestSpeed, minLevel: flate.HuffmanOnly, maxLevel: flate.BestCompression}
}

// Negotiate selects a level supported by both the server policy and client
// range. A nil preferred level uses the server default; an out-of-range
// preferred level is clamped to the common range.
func (policy CompressionLevelPolicy) Negotiate(clientMin, clientMax int, preferred *int) (int, error) {
	if !validNegotiatedCompressionLevel(clientMin) || !validNegotiatedCompressionLevel(clientMax) || clientMin > clientMax || policy.minLevel > policy.maxLevel {
		return 0, ErrCompressionLevelPolicyInvalid
	}
	minLevel := policy.minLevel
	if clientMin > minLevel {
		minLevel = clientMin
	}
	maxLevel := policy.maxLevel
	if clientMax < maxLevel {
		maxLevel = clientMax
	}
	if minLevel > maxLevel {
		return 0, ErrCompressionLevelPolicyInvalid
	}
	level := policy.defaultLevel
	if preferred != nil {
		if !validNegotiatedCompressionLevel(*preferred) {
			return 0, ErrCompressionLevelPolicyInvalid
		}
		level = *preferred
	}
	if level < minLevel {
		level = minLevel
	}
	if level > maxLevel {
		level = maxLevel
	}
	return level, nil
}

func validNegotiatedCompressionLevel(level int) bool {
	return level >= flate.HuffmanOnly && level <= flate.BestCompression && level != flate.NoCompression
}

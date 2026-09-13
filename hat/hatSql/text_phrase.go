package hatSql

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode"
)

const maxSQLTextProximityGap = 1 << 20

// TextTokenPosition is one normalized token and its zero-based position in a
// text value. Unlike TextTokens, repeated tokens are retained.
type TextTokenPosition struct {
	Token    string
	Position int
}

// TextTokenPositions returns normalized tokens in source order, preserving
// repeated tokens so callers can implement phrase and proximity matching.
func TextTokenPositions(value string) []TextTokenPosition {
	parts := strings.FieldsFunc(strings.ToLower(value), func(character rune) bool {
		return !unicode.IsLetter(character) && !unicode.IsNumber(character)
	})
	if len(parts) == 0 {
		return nil
	}
	positions := make([]TextTokenPosition, len(parts))
	for index, token := range parts {
		positions[index] = TextTokenPosition{Token: token, Position: index}
	}
	return positions
}

func textContainsPhrase(value, query string) bool {
	return textContainsProximity(value, query, 0)
}

// textContainsProximity matches query tokens in order. maxGap is the maximum
// number of intervening tokens allowed between consecutive query tokens.
func textContainsProximity(value, query string, maxGap int) bool {
	if maxGap < 0 {
		return false
	}
	valueTokens := TextTokenPositions(value)
	queryTokens := TextTokenPositions(query)
	if len(valueTokens) == 0 || len(queryTokens) == 0 {
		return false
	}
	for start, valueToken := range valueTokens {
		if valueToken.Token != queryTokens[0].Token {
			continue
		}
		valueIndex := start
		lastPosition := valueToken.Position
		matched := true
		for queryIndex := 1; queryIndex < len(queryTokens); queryIndex++ {
			found := false
			for valueIndex++; valueIndex < len(valueTokens); valueIndex++ {
				positionGap := valueTokens[valueIndex].Position - lastPosition - 1
				if positionGap > maxGap {
					break
				}
				if valueTokens[valueIndex].Token == queryTokens[queryIndex].Token {
					lastPosition = valueTokens[valueIndex].Position
					found = true
					break
				}
			}
			if !found {
				matched = false
				break
			}
		}
		if matched {
			return true
		}
	}
	return false
}

func sqlTextProximityGap(value interface{}) (int, error) {
	var gap int64
	switch typed := value.(type) {
	case int:
		gap = int64(typed)
	case int8:
		gap = int64(typed)
	case int16:
		gap = int64(typed)
	case int32:
		gap = int64(typed)
	case int64:
		gap = typed
	case uint:
		if uint64(typed) > maxSQLTextProximityGap {
			return 0, sqlTextProximityGapError()
		}
		gap = int64(typed)
	case uint8:
		gap = int64(typed)
	case uint16:
		gap = int64(typed)
	case uint32:
		if uint64(typed) > maxSQLTextProximityGap {
			return 0, sqlTextProximityGapError()
		}
		gap = int64(typed)
	case uint64:
		if typed > maxSQLTextProximityGap {
			return 0, sqlTextProximityGapError()
		}
		gap = int64(typed)
	case float32:
		if math.IsNaN(float64(typed)) || math.IsInf(float64(typed), 0) || float32(math.Trunc(float64(typed))) != typed {
			return 0, sqlTextProximityGapError()
		}
		if typed > maxSQLTextProximityGap || typed < 0 {
			return 0, sqlTextProximityGapError()
		}
		gap = int64(typed)
	case float64:
		if math.IsNaN(typed) || math.IsInf(typed, 0) || math.Trunc(typed) != typed || typed > maxSQLTextProximityGap || typed < 0 {
			return 0, sqlTextProximityGapError()
		}
		gap = int64(typed)
	case json.Number:
		parsed, err := strconv.ParseInt(string(typed), 10, 64)
		if err != nil {
			return 0, sqlTextProximityGapError()
		}
		gap = parsed
	default:
		return 0, sqlTextProximityGapError()
	}
	if gap < 0 || gap > maxSQLTextProximityGap {
		return 0, sqlTextProximityGapError()
	}
	return int(gap), nil
}

func sqlTextProximityGapError() error {
	return fmt.Errorf("CONTAINS_PROXIMITY distance must be a non-negative integer no larger than %d", maxSQLTextProximityGap)
}

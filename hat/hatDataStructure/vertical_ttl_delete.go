package hatDataStructure

import (
	"errors"
	"math/bits"
	"time"
)

var (
	// ErrVerticalTTLDeleteInvalid reports invalid dimensions, time, or bounds.
	ErrVerticalTTLDeleteInvalid = errors.New("hatDataStructure: invalid vertical TTL delete input")
)

// VerticalTTLDeleteCandidate identifies a physical row that was logically
// deleted by ApplyVerticalTTLDeletes. Key is copied as a string header only;
// the method does not read or retain any payload columns.
type VerticalTTLDeleteCandidate struct {
	Row uint64
	Key string
}

// VerticalTTLDeleteResult reports the narrow key-column work performed by a
// vertical TTL pass.
type VerticalTTLDeleteResult struct {
	Candidates []VerticalTTLDeleteCandidate
	Deleted    int
}

// ApplyVerticalTTLDeletes marks live rows whose non-zero expiry timestamp is
// at or before now. It reads only the delete bitmap, the key column, and the
// narrow expiry column. A zero expiry means that the row has no TTL. The
// payload columns remain untouched for a later physical compaction.
//
// maxRows bounds one maintenance pass; zero processes every matching row.
// Input dimensions are validated before any bitmap bit is changed.
func (bitmap *PersistentDeleteBitmap) ApplyVerticalTTLDeletes(
	keys []string,
	expiryUnixNano []int64,
	now time.Time,
	maxRows int,
) (VerticalTTLDeleteResult, error) {
	return bitmap.ApplyVerticalTTLDeletesInto(nil, keys, expiryUnixNano, now, maxRows)
}

// ApplyVerticalTTLDeletesInto is the reusable-buffer form of
// ApplyVerticalTTLDeletes. It clears candidates to length zero before the
// scan and reuses its backing array when capacity permits, so a maintenance
// loop can avoid per-pass result allocations.
func (bitmap *PersistentDeleteBitmap) ApplyVerticalTTLDeletesInto(
	candidates []VerticalTTLDeleteCandidate,
	keys []string,
	expiryUnixNano []int64,
	now time.Time,
	maxRows int,
) (VerticalTTLDeleteResult, error) {
	if bitmap == nil || now.IsZero() || maxRows < 0 || uint64(len(keys)) != bitmap.rows || uint64(len(expiryUnixNano)) != bitmap.rows {
		return VerticalTTLDeleteResult{}, ErrVerticalTTLDeleteInvalid
	}
	if candidates == nil {
		candidates = make([]VerticalTTLDeleteCandidate, 0, verticalTTLDeleteCapacity(maxRows))
	} else {
		candidates = candidates[:0]
	}
	result := VerticalTTLDeleteResult{Candidates: candidates}
	nowUnixNano := now.UnixNano()
	for wordIndex, deletedWord := range bitmap.words {
		liveWord := ^deletedWord
		if wordIndex == len(bitmap.words)-1 && bitmap.rows%64 != 0 {
			liveWord &= (uint64(1) << uint(bitmap.rows%64)) - 1
		}
		for liveWord != 0 {
			bit := uint(bits.TrailingZeros64(liveWord))
			row := uint64(wordIndex*64 + int(bit))
			expiry := expiryUnixNano[row]
			if expiry != 0 && expiry <= nowUnixNano {
				result.Candidates = append(result.Candidates, VerticalTTLDeleteCandidate{Row: row, Key: keys[row]})
				if maxRows > 0 && len(result.Candidates) >= maxRows {
					break
				}
			}
			liveWord &^= uint64(1) << bit
		}
		if maxRows > 0 && len(result.Candidates) >= maxRows {
			break
		}
	}
	for _, candidate := range result.Candidates {
		if changed, err := bitmap.Delete(candidate.Row); err != nil {
			return VerticalTTLDeleteResult{}, err
		} else if changed {
			result.Deleted++
		}
	}
	return result, nil
}

func verticalTTLDeleteCapacity(maxRows int) int {
	if maxRows > 0 && maxRows < 64 {
		return maxRows
	}
	return 64
}

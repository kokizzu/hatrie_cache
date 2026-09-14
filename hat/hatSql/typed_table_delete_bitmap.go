package hatSql

const typedTableDeleteBitmapWordBits = 64

// typedTableDeleteBitmap stores one logical-delete bit per physical row.
// The owning TypedTable holds its lock while accessing the bitmap.
type typedTableDeleteBitmap struct {
	words []uint64
}

func newTypedTableDeleteBitmap(rows int) typedTableDeleteBitmap {
	bitmap := typedTableDeleteBitmap{}
	bitmap.ensure(rows)
	return bitmap
}

func (bitmap *typedTableDeleteBitmap) ensure(rows int) {
	if bitmap == nil || rows <= 0 {
		return
	}
	words := (rows + typedTableDeleteBitmapWordBits - 1) / typedTableDeleteBitmapWordBits
	if words <= len(bitmap.words) {
		return
	}
	bitmap.words = append(bitmap.words, make([]uint64, words-len(bitmap.words))...)
}

func (bitmap *typedTableDeleteBitmap) set(index int) {
	if bitmap == nil || index < 0 {
		return
	}
	bitmap.ensure(index + 1)
	bitmap.words[index/typedTableDeleteBitmapWordBits] |= 1 << uint(index%typedTableDeleteBitmapWordBits)
}

func (bitmap *typedTableDeleteBitmap) clear(index int) {
	if bitmap == nil || index < 0 || index/typedTableDeleteBitmapWordBits >= len(bitmap.words) {
		return
	}
	bitmap.words[index/typedTableDeleteBitmapWordBits] &^= 1 << uint(index%typedTableDeleteBitmapWordBits)
}

func (bitmap typedTableDeleteBitmap) contains(index int) bool {
	if index < 0 || index/typedTableDeleteBitmapWordBits >= len(bitmap.words) {
		return false
	}
	return bitmap.words[index/typedTableDeleteBitmapWordBits]&(1<<uint(index%typedTableDeleteBitmapWordBits)) != 0
}

func (bitmap typedTableDeleteBitmap) liveWord(wordIndex, rows int) uint64 {
	if wordIndex < 0 || rows <= 0 {
		return 0
	}
	start := wordIndex * typedTableDeleteBitmapWordBits
	remaining := rows - start
	if remaining <= 0 {
		return 0
	}
	deleted := uint64(0)
	if wordIndex < len(bitmap.words) {
		deleted = bitmap.words[wordIndex]
	}
	live := ^deleted
	if remaining < typedTableDeleteBitmapWordBits {
		live &= (uint64(1) << uint(remaining)) - 1
	}
	return live
}

func (bitmap *typedTableDeleteBitmap) truncate(rows int) {
	if bitmap == nil || rows <= 0 {
		if bitmap != nil {
			bitmap.words = nil
		}
		return
	}
	words := (rows + typedTableDeleteBitmapWordBits - 1) / typedTableDeleteBitmapWordBits
	if words < len(bitmap.words) {
		bitmap.words = bitmap.words[:words]
	}
	if len(bitmap.words) == 0 || rows%typedTableDeleteBitmapWordBits == 0 {
		return
	}
	mask := uint64(1)<<uint(rows%typedTableDeleteBitmapWordBits) - 1
	bitmap.words[len(bitmap.words)-1] &= mask
}

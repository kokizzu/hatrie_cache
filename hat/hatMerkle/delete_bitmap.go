package hatMerkle

import (
	"errors"
	"fmt"
)

var (
	ErrPartDeleteBitmapInvalid  = errors.New("hatriecache: invalid immutable part delete bitmap")
	ErrPartDeleteBitmapMismatch = errors.New("hatriecache: immutable part delete bitmap does not match")
)

// PartDeleteBitmap identifies the logical-delete snapshot associated with an
// immutable part. The bitmap bytes stay outside the part catalog; the catalog
// retains only bounded row counts and the snapshot checksum.
type PartDeleteBitmap struct {
	RowCount     uint64
	DeletedCount uint64
	Snapshot     PartChecksum
}

// BuildPartDeleteBitmap creates validated metadata for one serialized delete
// bitmap snapshot.
func BuildPartDeleteBitmap(snapshot []byte, rowCount, deletedCount uint64) (PartDeleteBitmap, error) {
	bitmap := PartDeleteBitmap{
		RowCount:     rowCount,
		DeletedCount: deletedCount,
		Snapshot:     ChecksumPart(snapshot),
	}
	if err := bitmap.Validate(); err != nil {
		return PartDeleteBitmap{}, err
	}
	return bitmap, nil
}

// Validate checks metadata shape without reading the snapshot bytes.
func (bitmap PartDeleteBitmap) Validate() error {
	if bitmap.DeletedCount > bitmap.RowCount {
		return fmt.Errorf("%w: deleted row count exceeds row count", ErrPartDeleteBitmapInvalid)
	}
	if bitmap.Snapshot.Size == 0 {
		return fmt.Errorf("%w: snapshot is empty", ErrPartDeleteBitmapInvalid)
	}
	var zeroDigest [len(bitmap.Snapshot.Digest)]byte
	if bitmap.Snapshot.Digest == zeroDigest {
		return fmt.Errorf("%w: snapshot checksum is empty", ErrPartDeleteBitmapInvalid)
	}
	return nil
}

// Verify checks the serialized delete bitmap against its manifest metadata.
func (bitmap PartDeleteBitmap) Verify(snapshot []byte) error {
	if err := bitmap.Validate(); err != nil {
		return err
	}
	if !VerifyPartChecksum(snapshot, bitmap.Snapshot) {
		return ErrPartDeleteBitmapMismatch
	}
	return nil
}

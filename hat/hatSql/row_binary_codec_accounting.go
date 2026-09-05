package hatSql

import (
	"errors"
	"sync/atomic"
	"time"
)

var (
	ErrSQLRowBinaryCodecAccountingNegativeBytes = errors.New("hatSql: RowBinary accounting byte count cannot be negative")
	ErrSQLRowBinaryCodecAccountingNilCallback   = errors.New("hatSql: RowBinary accounting callback cannot be nil")
)

// SQLRowBinaryCodecAccounting collects opt-in size and synchronous operation-time
// measurements for a RowBinary encoder and decoder.
//
// The counters are safe to update from multiple goroutines. Failed callbacks
// contribute to operation and duration counters, but not byte totals.
type SQLRowBinaryCodecAccounting struct {
	logicalBytes     uint64
	encodedBytes     uint64
	encodeOperations uint64
	decodeOperations uint64
	encodeNanoseconds uint64
	decodeNanoseconds uint64
}

// SQLRowBinaryCodecAccountingSnapshot is a consistent point-in-time view of
// SQLRowBinaryCodecAccounting. CompressionRatio is logical bytes divided by
// encoded bytes, so values above one indicate smaller encoded output.
type SQLRowBinaryCodecAccountingSnapshot struct {
	LogicalBytes      uint64
	EncodedBytes      uint64
	EncodeOperations  uint64
	DecodeOperations  uint64
	EncodeNanoseconds uint64
	DecodeNanoseconds uint64
	CompressionRatio  float64
}

// MeasureEncode runs encode, records its synchronous duration, and accounts
// the resulting encoded size when encode succeeds.
func (a *SQLRowBinaryCodecAccounting) MeasureEncode(logicalBytes int, encode func() ([]byte, error)) ([]byte, error) {
	if logicalBytes < 0 {
		return nil, ErrSQLRowBinaryCodecAccountingNegativeBytes
	}
	if encode == nil {
		return nil, ErrSQLRowBinaryCodecAccountingNilCallback
	}

	started := time.Now()
	encoded, err := encode()
	elapsed := uint64(time.Since(started))
	atomic.AddUint64(&a.encodeOperations, 1)
	atomic.AddUint64(&a.encodeNanoseconds, elapsed)
	if err != nil {
		return nil, err
	}
	atomic.AddUint64(&a.logicalBytes, uint64(logicalBytes))
	atomic.AddUint64(&a.encodedBytes, uint64(len(encoded)))
	return encoded, nil
}

// MeasureDecode runs decode, records its synchronous duration, and accounts
// the input and logical sizes when decode succeeds. encoded may be empty.
func (a *SQLRowBinaryCodecAccounting) MeasureDecode(encoded []byte, logicalBytes int, decode func([]byte) error) error {
	if logicalBytes < 0 {
		return ErrSQLRowBinaryCodecAccountingNegativeBytes
	}
	if decode == nil {
		return ErrSQLRowBinaryCodecAccountingNilCallback
	}

	started := time.Now()
	err := decode(encoded)
	elapsed := uint64(time.Since(started))
	atomic.AddUint64(&a.decodeOperations, 1)
	atomic.AddUint64(&a.decodeNanoseconds, elapsed)
	if err != nil {
		return err
	}
	atomic.AddUint64(&a.logicalBytes, uint64(logicalBytes))
	atomic.AddUint64(&a.encodedBytes, uint64(len(encoded)))
	return nil
}

// Snapshot returns the current accounting counters and derived compression
// ratio. A zero encoded-byte total has a zero ratio to keep the result finite.
func (a *SQLRowBinaryCodecAccounting) Snapshot() SQLRowBinaryCodecAccountingSnapshot {
	logicalBytes := atomic.LoadUint64(&a.logicalBytes)
	encodedBytes := atomic.LoadUint64(&a.encodedBytes)
	snapshot := SQLRowBinaryCodecAccountingSnapshot{
		LogicalBytes:      logicalBytes,
		EncodedBytes:      encodedBytes,
		EncodeOperations:  atomic.LoadUint64(&a.encodeOperations),
		DecodeOperations:  atomic.LoadUint64(&a.decodeOperations),
		EncodeNanoseconds: atomic.LoadUint64(&a.encodeNanoseconds),
		DecodeNanoseconds: atomic.LoadUint64(&a.decodeNanoseconds),
	}
	if encodedBytes != 0 {
		snapshot.CompressionRatio = float64(logicalBytes) / float64(encodedBytes)
	}
	return snapshot
}

// Reset clears all counters. It is safe to call while other goroutines record
// measurements; updates racing with Reset may appear before or after the reset.
func (a *SQLRowBinaryCodecAccounting) Reset() {
	atomic.StoreUint64(&a.logicalBytes, 0)
	atomic.StoreUint64(&a.encodedBytes, 0)
	atomic.StoreUint64(&a.encodeOperations, 0)
	atomic.StoreUint64(&a.decodeOperations, 0)
	atomic.StoreUint64(&a.encodeNanoseconds, 0)
	atomic.StoreUint64(&a.decodeNanoseconds, 0)
}

package hatSql

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash"
	"strings"
)

const (
	// MaxSQLSinkIdempotencyBatchBytes bounds the batch payload hashed into one
	// derived token. The payload is never retained by the token API.
	MaxSQLSinkIdempotencyBatchBytes = 16 << 20
	// MaxSQLSinkIdempotencyProgress bounds frontier entries hashed into one
	// token.
	MaxSQLSinkIdempotencyProgress    = maxSQLSinkExactlyOnceProgress
	maxSQLSinkIdempotencyStringBytes = maxSQLSinkExactlyOnceStringBytes
	maxSQLSinkIdempotencyStackBytes  = 1024
	sqlSinkIdempotencyDomain         = "hatrie-cache/sql-sink-idempotency/v1"
)

var (
	// ErrSQLSinkIdempotencyTokenInvalid reports malformed sink or frontier
	// input.
	ErrSQLSinkIdempotencyTokenInvalid = errors.New("SQL sink idempotency token input is invalid")
	// ErrSQLSinkIdempotencyTokenLimit reports an input over a configured bound.
	ErrSQLSinkIdempotencyTokenLimit = errors.New("SQL sink idempotency token input exceeds its limit")
)

// SQLSinkIdempotencyTokenInput identifies the stable sink batch identity used
// to derive an idempotency key. Progress order is canonicalized, so retries
// may provide the same frontier entries in a different order.
type SQLSinkIdempotencyTokenInput struct {
	Sink     string            `json:"sink"`
	Progress []SQLSinkProgress `json:"progress"`
	Batch    []byte            `json:"-"`
}

// DeriveSQLSinkIdempotencyKey returns a stable 256-bit hexadecimal token for
// one sink, frontier, and batch. Transaction IDs are deliberately excluded so
// a regenerated attempt ID cannot turn the same logical batch into a second
// delivery. The caller should persist and pass the returned key to the sink.
func DeriveSQLSinkIdempotencyKey(input SQLSinkIdempotencyTokenInput) (string, error) {
	sink := strings.TrimSpace(input.Sink)
	if sink == "" || len(sink) > maxSQLSinkIdempotencyStringBytes {
		return "", ErrSQLSinkIdempotencyTokenInvalid
	}
	if len(input.Progress) == 0 {
		return "", ErrSQLSinkIdempotencyTokenInvalid
	}
	if len(input.Progress) > MaxSQLSinkIdempotencyProgress {
		return "", ErrSQLSinkIdempotencyTokenLimit
	}
	if len(input.Batch) > MaxSQLSinkIdempotencyBatchBytes {
		return "", ErrSQLSinkIdempotencyTokenLimit
	}

	var stackProgress [8]SQLSinkProgress
	progress := stackProgress[:0]
	if len(input.Progress) > len(stackProgress) {
		progress = make([]SQLSinkProgress, len(input.Progress))
	} else {
		progress = stackProgress[:len(input.Progress)]
	}
	for index, value := range input.Progress {
		value.Sink = strings.TrimSpace(value.Sink)
		value.Partition = strings.TrimSpace(value.Partition)
		if value.Sink != sink || value.Sink == "" || value.Partition == "" || len(value.Partition) > maxSQLSinkIdempotencyStringBytes {
			return "", ErrSQLSinkIdempotencyTokenInvalid
		}
		progress[index] = value
	}
	for index := 1; index < len(progress); index++ {
		value := progress[index]
		position := index
		for position > 0 && progress[position-1].Partition > value.Partition {
			progress[position] = progress[position-1]
			position--
		}
		progress[position] = value
	}
	for index := 1; index < len(progress); index++ {
		if progress[index-1].Partition == progress[index].Partition {
			return "", ErrSQLSinkIdempotencyTokenInvalid
		}
	}

	canonicalBytes := sqlSinkIdempotencyCanonicalBytes(sink, progress, input.Batch)
	if canonicalBytes <= maxSQLSinkIdempotencyStackBytes {
		var encodedInput [maxSQLSinkIdempotencyStackBytes]byte
		encoded := appendSQLSinkIdempotencyCanonical(nil, encodedInput[:0], sink, progress, input.Batch)
		digest := sha256.Sum256(encoded)
		return encodeSQLSinkIdempotencyDigest(digest), nil
	}

	// Avoid copying large batches into a second canonical buffer. The small
	// path above covers normal sink batches and keeps its transient footprint on
	// the stack; this path streams bounded payloads directly into SHA-256.
	digest := sha256.New()
	writeSQLSinkIdempotencyTokenBytes(digest, []byte(sqlSinkIdempotencyDomain))
	writeSQLSinkIdempotencyTokenBytes(digest, []byte(sink))
	writeSQLSinkIdempotencyTokenUvarint(digest, uint64(len(progress)))
	for _, value := range progress {
		writeSQLSinkIdempotencyTokenBytes(digest, []byte(value.Partition))
		writeSQLSinkIdempotencyTokenUvarint(digest, value.Frontier)
	}
	writeSQLSinkIdempotencyTokenBytes(digest, input.Batch)
	var sum [sha256.Size]byte
	digest.Sum(sum[:0])
	return encodeSQLSinkIdempotencyDigest(sum), nil
}

// DeriveSQLSinkCommitIdempotencyKey returns a detached commit with its
// IdempotencyKey replaced by the canonical token derived from its sink,
// progress, and batch. Existing caller-supplied keys are intentionally
// replaced to make the derivation unambiguous.
func DeriveSQLSinkCommitIdempotencyKey(commit SQLSinkCommit, batch []byte) (SQLSinkCommit, error) {
	key, err := DeriveSQLSinkIdempotencyKey(SQLSinkIdempotencyTokenInput{
		Sink:     commit.Sink,
		Progress: commit.Progress,
		Batch:    batch,
	})
	if err != nil {
		return SQLSinkCommit{}, err
	}
	commit.Sink = strings.TrimSpace(commit.Sink)
	commit.Progress = cloneSQLSinkProgress(commit.Progress)
	commit.IdempotencyKey = key
	return commit, nil
}

func writeSQLSinkIdempotencyTokenBytes(destination hash.Hash, value []byte) {
	writeSQLSinkIdempotencyTokenUvarint(destination, uint64(len(value)))
	_, _ = destination.Write(value)
}

func writeSQLSinkIdempotencyTokenUvarint(destination hash.Hash, value uint64) {
	var encoded [binary.MaxVarintLen64]byte
	length := binary.PutUvarint(encoded[:], value)
	_, _ = destination.Write(encoded[:length])
}

func sqlSinkIdempotencyCanonicalBytes(sink string, progress []SQLSinkProgress, batch []byte) int {
	length := sqlSinkIdempotencyUvarintLength(uint64(len(sqlSinkIdempotencyDomain))) + len(sqlSinkIdempotencyDomain)
	length += sqlSinkIdempotencyUvarintLength(uint64(len(sink))) + len(sink)
	length += sqlSinkIdempotencyUvarintLength(uint64(len(progress)))
	for _, value := range progress {
		length += sqlSinkIdempotencyUvarintLength(uint64(len(value.Partition))) + len(value.Partition)
		length += sqlSinkIdempotencyUvarintLength(value.Frontier)
	}
	return length + sqlSinkIdempotencyUvarintLength(uint64(len(batch))) + len(batch)
}

func appendSQLSinkIdempotencyCanonical(destination []byte, prefix []byte, sink string, progress []SQLSinkProgress, batch []byte) []byte {
	destination = prefix
	destination = appendSQLSinkIdempotencyString(destination, sqlSinkIdempotencyDomain)
	destination = appendSQLSinkIdempotencyString(destination, sink)
	destination = appendSQLSinkIdempotencyTokenUvarint(destination, uint64(len(progress)))
	for _, value := range progress {
		destination = appendSQLSinkIdempotencyString(destination, value.Partition)
		destination = appendSQLSinkIdempotencyTokenUvarint(destination, value.Frontier)
	}
	return appendSQLSinkIdempotencyBytes(destination, batch)
}

func appendSQLSinkIdempotencyString(destination []byte, value string) []byte {
	destination = appendSQLSinkIdempotencyTokenUvarint(destination, uint64(len(value)))
	return append(destination, value...)
}

func appendSQLSinkIdempotencyBytes(destination, value []byte) []byte {
	destination = appendSQLSinkIdempotencyTokenUvarint(destination, uint64(len(value)))
	return append(destination, value...)
}

func appendSQLSinkIdempotencyTokenUvarint(destination []byte, value uint64) []byte {
	var encoded [binary.MaxVarintLen64]byte
	length := binary.PutUvarint(encoded[:], value)
	return append(destination, encoded[:length]...)
}

func sqlSinkIdempotencyUvarintLength(value uint64) int {
	length := 1
	for value >= 0x80 {
		value >>= 7
		length++
	}
	return length
}

func encodeSQLSinkIdempotencyDigest(digest [sha256.Size]byte) string {
	var encoded [sha256.Size * 2]byte
	hex.Encode(encoded[:], digest[:])
	return string(encoded[:])
}

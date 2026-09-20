package hatDataStructure

import (
	"bytes"
	"errors"
	"testing"
	"time"
)

func TestTTLRecompressionSeparatesRewriteFromDelete(t *testing.T) {
	createdAt := time.Unix(100, 0)
	policy := TTLRecompressionPolicy{
		RecompressAfter: 6 * time.Hour,
		DeleteAfter:     24 * time.Hour,
	}
	if got := policy.Decide(createdAt.Add(12*time.Hour), createdAt); got != TTLRecompressionKeep {
		t.Fatalf("future record decision = %v, want keep", got)
	}
	if got := policy.Decide(createdAt, createdAt.Add(12*time.Hour)); got != TTLRecompressionRewrite {
		t.Fatalf("aged record decision = %v, want rewrite", got)
	}
	if got := policy.Decide(createdAt, createdAt.Add(48*time.Hour)); got != TTLRecompressionDelete {
		t.Fatalf("expired record decision = %v, want delete", got)
	}

	hot, err := NewTupleCompressor(TupleCompressionOptions{Algorithm: TupleCompressionNone})
	if err != nil {
		t.Fatalf("NewTupleCompressor(hot) error = %v", err)
	}
	cold, err := NewTupleCompressor(DefaultTupleCompressionOptions())
	if err != nil {
		t.Fatalf("NewTupleCompressor(cold) error = %v", err)
	}
	payload := bytes.Repeat([]byte("recompression-policy-payload-"), 1024)
	frame, err := hot.Compress(payload)
	if err != nil {
		t.Fatalf("hot Compress() error = %v", err)
	}
	rewritten, decision, err := hot.RecompressIfDue(
		frame,
		createdAt,
		createdAt.Add(12*time.Hour),
		policy,
		cold,
	)
	if err != nil {
		t.Fatalf("RecompressIfDue() error = %v", err)
	}
	if decision != TTLRecompressionRewrite {
		t.Fatalf("rewrite decision = %v, want rewrite", decision)
	}
	info, err := InspectTupleCompressionFrame(rewritten)
	if err != nil {
		t.Fatalf("InspectTupleCompressionFrame() error = %v", err)
	}
	if info.Algorithm != TupleCompressionZSTD {
		t.Fatalf("rewritten algorithm = %v, want zstd", info.Algorithm)
	}
	decoded, err := cold.Decompress(rewritten)
	if err != nil {
		t.Fatalf("cold Decompress() error = %v", err)
	}
	if !bytes.Equal(decoded, payload) {
		t.Fatal("recompressed payload changed")
	}
}

func TestTTLRecompressionDeleteSkipsFrameDecode(t *testing.T) {
	compressor, err := NewTupleCompressor(TupleCompressionOptions{Algorithm: TupleCompressionNone})
	if err != nil {
		t.Fatalf("NewTupleCompressor() error = %v", err)
	}
	createdAt := time.Unix(200, 0)
	frame, decision, err := compressor.RecompressIfDue(
		[]byte("corrupt frame"),
		createdAt,
		createdAt.Add(2*time.Hour),
		TTLRecompressionPolicy{DeleteAfter: time.Hour},
		nil,
	)
	if err != nil {
		t.Fatalf("expired corrupt frame error = %v", err)
	}
	if decision != TTLRecompressionDelete || frame != nil {
		t.Fatalf("expired corrupt frame result = (%v, %q), want delete and nil", decision, frame)
	}
}

func TestTTLRecompressionRejectsInvalidPolicyAndMissingTarget(t *testing.T) {
	invalid := TTLRecompressionPolicy{RecompressAfter: time.Hour, DeleteAfter: time.Hour}
	if !errors.Is(invalid.Validate(), ErrTTLRecompressionInvalid) {
		t.Fatalf("invalid policy error = %v", invalid.Validate())
	}
	compressor, err := NewTupleCompressor(TupleCompressionOptions{Algorithm: TupleCompressionNone})
	if err != nil {
		t.Fatalf("NewTupleCompressor() error = %v", err)
	}
	createdAt := time.Unix(300, 0)
	frame, err := compressor.Compress([]byte("payload"))
	if err != nil {
		t.Fatalf("Compress() error = %v", err)
	}
	_, _, err = compressor.RecompressIfDue(
		frame,
		createdAt,
		createdAt.Add(2*time.Hour),
		TTLRecompressionPolicy{RecompressAfter: time.Hour},
		nil,
	)
	if !errors.Is(err, ErrTTLRecompressionTarget) {
		t.Fatalf("missing target error = %v", err)
	}
}

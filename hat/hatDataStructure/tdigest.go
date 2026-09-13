package hatDataStructure

import (
	"errors"
	"fmt"
	"math"
	"sort"

	json "github.com/goccy/go-json"
)

const (
	// DefaultTDigestCompression controls the default centroid budget.
	DefaultTDigestCompression uint32 = 100
	MinTDigestCompression     uint32 = 20
	MaxTDigestCompression     uint32 = 1000
)

// TDigestInfo reports the size and bounds of a compact TDigest.
type TDigestInfo struct {
	Compression  uint32  `json:"compression"`
	Count        uint64  `json:"count"`
	CentroidSize uint64  `json:"centroid_size"`
	Min          float64 `json:"min"`
	Max          float64 `json:"max"`
	RankError    uint64  `json:"rank_error"`
	EncodedBytes int64   `json:"encoded_bytes"`
}

// TDigestCentroid is one weighted mean in a TDigest snapshot.
type TDigestCentroid struct {
	Mean  float64 `json:"mean"`
	Count uint64  `json:"count"`
}

// TDigestSnapshot is the portable representation of a TDigest.
type TDigestSnapshot struct {
	Compression uint32            `json:"compression"`
	Count       uint64            `json:"count"`
	Centroids   []TDigestCentroid `json:"centroids"`
}

// TDigest tracks approximate quantiles with weighted centroids and increased
// resolution near both tails.
type TDigest struct {
	compression uint32
	count       uint64
	centroids   []TDigestCentroid
}

// NewTDigest constructs a digest with the requested centroid compression.
func NewTDigest(compression uint32) (TDigest, error) {
	if err := ValidateTDigestCompression(compression); err != nil {
		return TDigest{}, err
	}
	return TDigest{compression: compression}, nil
}

// NewDefaultTDigest constructs a digest with DefaultTDigestCompression.
func NewDefaultTDigest() TDigest {
	digest, err := NewTDigest(DefaultTDigestCompression)
	if err != nil {
		panic(err)
	}
	return digest
}

// ValidateTDigestCompression validates a centroid compression setting.
func ValidateTDigestCompression(compression uint32) error {
	if compression < MinTDigestCompression || compression > MaxTDigestCompression {
		return fmt.Errorf("hatriecache: t-digest compression must be between %d and %d", MinTDigestCompression, MaxTDigestCompression)
	}
	return nil
}

// ValidateTDigestSnapshot validates a portable digest snapshot before use.
func ValidateTDigestSnapshot(snapshot TDigestSnapshot) error {
	if err := ValidateTDigestCompression(snapshot.Compression); err != nil {
		return err
	}
	if snapshot.Count == 0 {
		if len(snapshot.Centroids) != 0 {
			return errors.New("hatriecache: empty t-digest snapshot must not contain centroids")
		}
		return nil
	}
	if len(snapshot.Centroids) == 0 {
		return errors.New("hatriecache: t-digest snapshot centroids are required")
	}
	if len(snapshot.Centroids) > int(snapshot.Compression)*8+10 {
		return errors.New("hatriecache: t-digest snapshot has too many centroids")
	}
	var total uint64
	for index, centroid := range snapshot.Centroids {
		if !IsFiniteQuantileValue(centroid.Mean) {
			return errors.New("hatriecache: t-digest centroid mean must be finite")
		}
		if centroid.Count == 0 {
			return errors.New("hatriecache: t-digest centroid count must be positive")
		}
		if index > 0 && centroid.Mean < snapshot.Centroids[index-1].Mean {
			return errors.New("hatriecache: t-digest centroids must be sorted")
		}
		if ^uint64(0)-total < centroid.Count {
			return errors.New("hatriecache: t-digest centroid count overflows total")
		}
		total += centroid.Count
	}
	if total != snapshot.Count {
		return errors.New("hatriecache: t-digest centroid counts do not match total count")
	}
	return nil
}

// NewTDigestFromSnapshot reconstructs a digest from a validated snapshot.
func NewTDigestFromSnapshot(snapshot TDigestSnapshot) (TDigest, error) {
	if err := ValidateTDigestSnapshot(snapshot); err != nil {
		return TDigest{}, err
	}
	digest := TDigest{
		compression: snapshot.Compression,
		count:       snapshot.Count,
		centroids:   make([]TDigestCentroid, len(snapshot.Centroids)),
	}
	copy(digest.centroids, snapshot.Centroids)
	return digest, nil
}

// Add adds one or more finite values and returns the current median estimate.
// Invalid input is rejected before any value is added.
func (digest *TDigest) Add(value float64, values ...float64) QuantileEstimate {
	if digest == nil || digest.compression == 0 || !validTDigestValues(value, values...) {
		return QuantileEstimate{}
	}
	digest.addValid(value)
	for _, value := range values {
		digest.addValid(value)
	}
	estimate, _ := digest.Estimate(0.5)
	return estimate
}

// AddValidBatch adds a batch of finite values without repeating validation.
func (digest *TDigest) AddValidBatch(values []float64) QuantileEstimate {
	if digest == nil || digest.compression == 0 || !validTDigestSlice(values) {
		return QuantileEstimate{}
	}
	for _, value := range values {
		digest.addValid(value)
	}
	estimate, _ := digest.Estimate(0.5)
	return estimate
}

func (digest *TDigest) addValid(value float64) {
	digest.count = saturatingAddUint64TDigest(digest.count, 1)
	if len(digest.centroids) == 0 {
		digest.centroids = append(digest.centroids, TDigestCentroid{Mean: value, Count: 1})
		return
	}

	insert := sort.Search(len(digest.centroids), func(index int) bool {
		return digest.centroids[index].Mean >= value
	})
	index := insert
	if index == len(digest.centroids) {
		index--
	} else if index > 0 {
		leftDistance := math.Abs(value - digest.centroids[index-1].Mean)
		rightDistance := math.Abs(digest.centroids[index].Mean - value)
		if leftDistance <= rightDistance {
			index--
		}
	}
	if digest.canMerge(index, 1) {
		centroid := &digest.centroids[index]
		oldCount := centroid.Count
		centroid.Mean = (centroid.Mean*float64(oldCount) + value) / float64(oldCount+1)
		centroid.Count++
	} else {
		digest.centroids = append(digest.centroids, TDigestCentroid{})
		copy(digest.centroids[insert+1:], digest.centroids[insert:])
		digest.centroids[insert] = TDigestCentroid{Mean: value, Count: 1}
	}
	digest.compactIfNeeded()
}

func (digest TDigest) canMerge(index int, additional uint64) bool {
	if index < 0 || index >= len(digest.centroids) || additional == 0 {
		return false
	}
	centroid := digest.centroids[index]
	if ^uint64(0)-centroid.Count < additional {
		return false
	}
	prefix := uint64(0)
	for _, previous := range digest.centroids[:index] {
		if ^uint64(0)-prefix < previous.Count {
			prefix = ^uint64(0)
			break
		}
		prefix += previous.Count
	}
	combined := centroid.Count + additional
	q := (float64(prefix) + float64(combined)/2) / float64(digest.count)
	return combined <= tdigestCentroidLimit(digest.compression, q)
}

func (digest *TDigest) compactIfNeeded() {
	maximum := int(digest.compression)*8 + 10
	if len(digest.centroids) > maximum {
		digest.compress()
	}
}

func (digest *TDigest) compress() {
	if len(digest.centroids) < 2 || digest.count == 0 {
		return
	}
	sort.SliceStable(digest.centroids, func(left, right int) bool {
		return digest.centroids[left].Mean < digest.centroids[right].Mean
	})
	merged := digest.centroids[:0]
	current := digest.centroids[0]
	prefix := uint64(0)
	for _, next := range digest.centroids[1:] {
		if ^uint64(0)-current.Count >= next.Count {
			combined := current.Count + next.Count
			q := (float64(prefix) + float64(combined)/2) / float64(digest.count)
			if combined <= tdigestCentroidLimit(digest.compression, q) {
				current.Mean = weightedTDigestMean(current, next)
				current.Count = combined
				continue
			}
		}
		merged = append(merged, current)
		prefix = saturatingAddUint64TDigest(prefix, current.Count)
		current = next
	}
	digest.centroids = append(merged, current)
}

func tdigestCentroidLimit(compression uint32, quantile float64) uint64 {
	if quantile < 0 {
		quantile = 0
	} else if quantile > 1 {
		quantile = 1
	}
	limit := 4 * float64(compression) * quantile * (1 - quantile)
	if limit < 1 {
		return 1
	}
	return uint64(math.Ceil(limit))
}

func weightedTDigestMean(left, right TDigestCentroid) float64 {
	total := float64(left.Count + right.Count)
	return left.Mean*(float64(left.Count)/total) + right.Mean*(float64(right.Count)/total)
}

// Estimate returns an approximate value for a quantile in [0, 1].
func (digest TDigest) Estimate(quantile float64) (QuantileEstimate, bool) {
	if len(digest.centroids) == 0 || digest.count == 0 {
		return QuantileEstimate{}, false
	}
	if quantile <= 0 {
		return digest.estimateFromValue(0, digest.centroids[0].Mean), true
	}
	last := digest.centroids[len(digest.centroids)-1]
	if quantile >= 1 {
		return digest.estimateFromValue(1, last.Mean), true
	}
	target := quantile * float64(digest.count)
	cumulative := float64(0)
	previous := digest.centroids[0]
	previousCenter := float64(previous.Count) / 2
	for index, centroid := range digest.centroids {
		center := cumulative + float64(centroid.Count)/2
		if target <= center {
			if index == 0 {
				return digest.estimateFromValue(quantile, centroid.Mean), true
			}
			if previous.Count == 1 || centroid.Count == 1 {
				return digest.estimateFromValue(quantile, centroid.Mean), true
			}
			span := center - previousCenter
			if span <= 0 {
				return digest.estimateFromValue(quantile, centroid.Mean), true
			}
			ratio := (target - previousCenter) / span
			if ratio < 0 {
				ratio = 0
			} else if ratio > 1 {
				ratio = 1
			}
			value := previous.Mean + ratio*(centroid.Mean-previous.Mean)
			return digest.estimateFromValue(quantile, value), true
		}
		cumulative += float64(centroid.Count)
		previous = centroid
		previousCenter = center
	}
	return digest.estimateFromValue(quantile, last.Mean), true
}

func (digest TDigest) estimateFromValue(quantile, value float64) QuantileEstimate {
	return QuantileEstimate{
		Quantile:  quantile,
		Value:     value,
		Count:     digest.count,
		RankError: digest.rankError(),
	}
}

// Merge combines another digest with the same compression setting.
func (digest *TDigest) Merge(other TDigest) error {
	if digest == nil || digest.compression == 0 {
		return errors.New("hatriecache: nil t-digest")
	}
	if err := ValidateTDigestSnapshot(other.Snapshot()); err != nil {
		return err
	}
	if other.compression != digest.compression {
		return errors.New("hatriecache: t-digest compression mismatch")
	}
	if other.count == 0 {
		return nil
	}
	if ^uint64(0)-digest.count < other.count {
		digest.count = ^uint64(0)
	} else {
		digest.count += other.count
	}
	digest.centroids = append(digest.centroids, other.centroids...)
	digest.compress()
	digest.compactIfNeeded()
	return nil
}

// Snapshot returns a detached portable digest representation.
func (digest TDigest) Snapshot() TDigestSnapshot {
	snapshot := TDigestSnapshot{
		Compression: digest.compression,
		Count:       digest.count,
		Centroids:   make([]TDigestCentroid, len(digest.centroids)),
	}
	copy(snapshot.Centroids, digest.centroids)
	return snapshot
}

// Info returns bounded-size digest metadata.
func (digest TDigest) Info() TDigestInfo {
	info := TDigestInfo{
		Compression:  digest.compression,
		Count:        digest.count,
		CentroidSize: uint64(len(digest.centroids)),
		RankError:    digest.rankError(),
		EncodedBytes: digest.EncodedSize(),
	}
	if len(digest.centroids) > 0 {
		info.Min = digest.centroids[0].Mean
		info.Max = digest.centroids[len(digest.centroids)-1].Mean
	}
	return info
}

// EncodedSize returns the JSON snapshot size in bytes.
func (digest TDigest) EncodedSize() int64 {
	data, err := json.Marshal(digest.Snapshot())
	if err != nil {
		return 0
	}
	return int64(len(data))
}

func (digest TDigest) rankError() uint64 {
	if digest.count == 0 || digest.compression == 0 {
		return 0
	}
	error := uint64(math.Ceil(float64(digest.count) / float64(digest.compression)))
	if error < 1 {
		return 1
	}
	return error
}

func validTDigestValues(value float64, values ...float64) bool {
	if !IsFiniteQuantileValue(value) {
		return false
	}
	return validTDigestSlice(values)
}

func validTDigestSlice(values []float64) bool {
	for _, value := range values {
		if !IsFiniteQuantileValue(value) {
			return false
		}
	}
	return true
}

func saturatingAddUint64TDigest(value, delta uint64) uint64 {
	if ^uint64(0)-value < delta {
		return ^uint64(0)
	}
	return value + delta
}

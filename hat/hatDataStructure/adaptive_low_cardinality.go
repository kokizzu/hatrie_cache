package hatDataStructure

const defaultAdaptiveLowCardinalityMinRows = 64

func newAdaptiveLowCardinalityStringBuilder(options LowCardinalityStringBuilderOptions) LowCardinalityStringBuilder {
	initial := options.InitialCapacity
	if initial < 0 {
		initial = 0
	}
	maxDistinct := options.MaxDistinctValues
	if maxDistinct == 0 {
		maxDistinct = DefaultLowCardinalityStringMaxDistinctValues
	} else if maxDistinct < -1 {
		maxDistinct = -1
	}
	initialRows := options.InitialRowCapacity
	if initialRows < 0 {
		initialRows = 0
	}
	return LowCardinalityStringBuilder{
		dictionary:  make([]string, 0, initial),
		lookup:      make(map[string]uint32, initial),
		codes:       make([]uint32, 0, initialRows),
		maxDistinct: maxDistinct,
	}
}

// AdaptiveLowCardinalityStringBuilderOptions controls the opt-in adaptive
// dictionary builder. A zero MaxDistinctValues uses the existing conservative
// dictionary limit. A negative limit disables that limit. MaxDistinctRatio is
// disabled when zero and causes fallback once the input has at least
// MinRowsBeforeFallback rows and the dictionary ratio is strictly higher.
type AdaptiveLowCardinalityStringBuilderOptions struct {
	InitialCapacity       int
	InitialRowCapacity    int
	MaxDistinctValues     int
	MaxDistinctRatio      float64
	MinRowsBeforeFallback int
}

// AdaptiveLowCardinalityStringBuilder keeps low-cardinality input in the
// existing sorted dictionary representation and switches to an exact raw
// representation when dictionary admission is no longer economical. The
// switch is monotone, so readers never observe mixed representations.
type AdaptiveLowCardinalityStringBuilder struct {
	dictionary      LowCardinalityStringBuilder
	dictionaryReady bool
	options         AdaptiveLowCardinalityStringBuilderOptions
	sampleValues    []string
	sampleValid     []uint64
	sampleSeen      map[string]struct{}
	sampling        bool
	rawValues       []string
	rawValid        []uint64
	fallback        bool
	built           bool
}

// NewAdaptiveLowCardinalityStringBuilder creates an opt-in adaptive builder.
// Negative capacities are handled by the underlying builder and cannot cause
// a panic.
func NewAdaptiveLowCardinalityStringBuilder(options AdaptiveLowCardinalityStringBuilderOptions) *AdaptiveLowCardinalityStringBuilder {
	minRows := options.MinRowsBeforeFallback
	if minRows <= 0 {
		minRows = defaultAdaptiveLowCardinalityMinRows
	}
	ratio := options.MaxDistinctRatio
	if ratio <= 0 || ratio > 1 {
		ratio = 0
	}
	options.MinRowsBeforeFallback = minRows
	options.MaxDistinctRatio = ratio
	builder := &AdaptiveLowCardinalityStringBuilder{options: options}
	if ratio == 0 {
		builder.dictionary = newAdaptiveLowCardinalityStringBuilder(LowCardinalityStringBuilderOptions{
			InitialCapacity:    options.InitialCapacity,
			InitialRowCapacity: options.InitialRowCapacity,
			MaxDistinctValues:  options.MaxDistinctValues,
		})
		builder.dictionaryReady = true
		return builder
	}
	builder.sampleValues = make([]string, 0, minRows)
	builder.sampleSeen = make(map[string]struct{}, minRows)
	builder.sampling = true
	return builder
}

// Append adds one non-NULL value. Exceeding either adaptive threshold falls
// back to raw values and still accepts the row instead of returning a
// distinct-value error.
func (builder *AdaptiveLowCardinalityStringBuilder) Append(value string) error {
	if builder == nil || builder.built {
		return ErrLowCardinalityStringBuilderBuilt
	}
	if builder.sampling {
		return builder.appendSample(value, true)
	}
	if builder.fallback {
		builder.rawValues = append(builder.rawValues, value)
		return nil
	}
	if _, err := builder.dictionary.Append(value); err != nil {
		if err != ErrLowCardinalityStringDistinctLimit {
			return err
		}
		if err := builder.fallbackToRaw(); err != nil {
			return err
		}
		builder.rawValues = append(builder.rawValues, value)
		return nil
	}
	return builder.maybeFallback()
}

// AppendNull adds one NULL row without adding a dictionary value.
func (builder *AdaptiveLowCardinalityStringBuilder) AppendNull() error {
	if builder == nil || builder.built {
		return ErrLowCardinalityStringBuilderBuilt
	}
	if builder.sampling {
		return builder.appendSample("", false)
	}
	if builder.fallback {
		row := len(builder.rawValues)
		builder.rawValues = append(builder.rawValues, "")
		builder.setRawValid(row, false)
		return nil
	}
	if err := builder.dictionary.AppendNull(); err != nil {
		return err
	}
	return builder.maybeFallback()
}

// Build seals the adaptive column. The returned representation owns its
// backing slices and is immutable from the builder's perspective.
func (builder *AdaptiveLowCardinalityStringBuilder) Build() (AdaptiveLowCardinalityStringColumn, error) {
	if builder == nil || builder.built {
		return AdaptiveLowCardinalityStringColumn{}, ErrLowCardinalityStringBuilderBuilt
	}
	if builder.sampling {
		if err := builder.finishSample(); err != nil {
			return AdaptiveLowCardinalityStringColumn{}, err
		}
	}
	builder.built = true
	if !builder.fallback {
		column, err := builder.dictionary.Build()
		if err != nil {
			builder.built = false
			return AdaptiveLowCardinalityStringColumn{}, err
		}
		builder.dictionary = LowCardinalityStringBuilder{}
		builder.dictionaryReady = false
		return AdaptiveLowCardinalityStringColumn{dictionary: column, dictionaryMode: true}, nil
	}
	column := AdaptiveLowCardinalityStringColumn{
		rawValues:   builder.rawValues,
		rawValid:    builder.rawValid,
		cardinality: -1,
	}
	builder.rawValues = nil
	builder.rawValid = nil
	return column, nil
}

func (builder *AdaptiveLowCardinalityStringBuilder) maybeFallback() error {
	if builder.options.MaxDistinctRatio <= 0 || len(builder.dictionary.codes) < builder.options.MinRowsBeforeFallback {
		return nil
	}
	if float64(len(builder.dictionary.dictionary))/float64(len(builder.dictionary.codes)) <= builder.options.MaxDistinctRatio {
		return nil
	}
	return builder.fallbackToRaw()
}

func (builder *AdaptiveLowCardinalityStringBuilder) appendSample(value string, valid bool) error {
	row := len(builder.sampleValues)
	builder.sampleValues = append(builder.sampleValues, value)
	if valid {
		builder.sampleSeen[value] = struct{}{}
		if builder.sampleValid != nil {
			if words := (row + 64) / 64; len(builder.sampleValid) < words {
				builder.sampleValid = append(builder.sampleValid, make([]uint64, words-len(builder.sampleValid))...)
			}
			builder.sampleValid[row>>6] |= uint64(1) << uint(row&63)
		}
	} else {
		if builder.sampleValid == nil {
			builder.sampleValid = make([]uint64, (row+64)/64)
			for index := 0; index < row; index++ {
				builder.sampleValid[index>>6] |= uint64(1) << uint(index&63)
			}
		} else if words := (row + 64) / 64; len(builder.sampleValid) < words {
			builder.sampleValid = append(builder.sampleValid, make([]uint64, words-len(builder.sampleValid))...)
		}
	}
	if len(builder.sampleValues) < builder.options.MinRowsBeforeFallback {
		return nil
	}
	return builder.finishSample()
}

func (builder *AdaptiveLowCardinalityStringBuilder) finishSample() error {
	if !builder.sampling {
		return nil
	}
	ratio := 0.0
	if len(builder.sampleValues) != 0 {
		ratio = float64(len(builder.sampleSeen)) / float64(len(builder.sampleValues))
	}
	if (builder.options.MaxDistinctValues > 0 && len(builder.sampleSeen) > builder.options.MaxDistinctValues) || ratio > builder.options.MaxDistinctRatio {
		builder.rawValues = builder.sampleValues
		builder.rawValid = builder.sampleValid
		builder.sampleValues = nil
		builder.sampleValid = nil
		builder.sampleSeen = nil
		builder.sampling = false
		builder.fallback = true
		return nil
	}
	builder.dictionary = newAdaptiveLowCardinalityStringBuilder(LowCardinalityStringBuilderOptions{
		InitialCapacity:    builder.options.InitialCapacity,
		InitialRowCapacity: builder.options.InitialRowCapacity,
		MaxDistinctValues:  builder.options.MaxDistinctValues,
	})
	builder.dictionaryReady = true
	for row, value := range builder.sampleValues {
		if adaptiveRawRowValid(builder.sampleValid, row) {
			if _, err := builder.dictionary.Append(value); err != nil {
				return err
			}
			continue
		}
		if err := builder.dictionary.AppendNull(); err != nil {
			return err
		}
	}
	builder.sampleValues = nil
	builder.sampleValid = nil
	builder.sampleSeen = nil
	builder.sampling = false
	return nil
}

func (builder *AdaptiveLowCardinalityStringBuilder) fallbackToRaw() error {
	if builder.fallback {
		return nil
	}
	values := make([]string, len(builder.dictionary.codes))
	for row, code := range builder.dictionary.codes {
		if code >= uint32(len(builder.dictionary.dictionary)) {
			return ErrLowCardinalityStringBinaryInvalid
		}
		if builder.dictionary.valid == nil || builder.dictionary.valid[row>>6]&(uint64(1)<<uint(row&63)) != 0 {
			values[row] = builder.dictionary.dictionary[code]
		}
	}
	builder.rawValues = values
	builder.rawValid = append([]uint64(nil), builder.dictionary.valid...)
	builder.dictionary = LowCardinalityStringBuilder{}
	builder.dictionaryReady = false
	builder.fallback = true
	return nil
}

func (builder *AdaptiveLowCardinalityStringBuilder) setRawValid(row int, valid bool) {
	if builder.rawValid == nil {
		if valid {
			return
		}
		builder.rawValid = make([]uint64, (row+64)/64)
		for index := 0; index < row; index++ {
			builder.rawValid[index>>6] |= uint64(1) << uint(index&63)
		}
		return
	}
	words := (row + 64) / 64
	if len(builder.rawValid) < words {
		builder.rawValid = append(builder.rawValid, make([]uint64, words-len(builder.rawValid))...)
	}
	if valid {
		builder.rawValid[row>>6] |= uint64(1) << uint(row&63)
	}
}

// AdaptiveLowCardinalityStringColumn is either a sorted dictionary column or
// an exact raw fallback. It exposes common value operations while making the
// representation visible to callers and explain/metrics code.
type AdaptiveLowCardinalityStringColumn struct {
	dictionary     LowCardinalityStringColumn
	dictionaryMode bool
	rawValues      []string
	rawValid       []uint64
	cardinality    int
}

// UsesDictionary reports whether the column retained compact dictionary codes.
func (column AdaptiveLowCardinalityStringColumn) UsesDictionary() bool {
	return column.dictionaryMode
}

// Len returns the number of rows, including NULL rows.
func (column AdaptiveLowCardinalityStringColumn) Len() int {
	if column.dictionaryMode {
		return column.dictionary.Len()
	}
	return len(column.rawValues)
}

// Cardinality returns the number of distinct non-NULL values.
func (column AdaptiveLowCardinalityStringColumn) Cardinality() int {
	if column.dictionaryMode {
		return column.dictionary.Cardinality()
	}
	if column.cardinality < 0 {
		return adaptiveRawCardinality(column.rawValues, column.rawValid)
	}
	return column.cardinality
}

// CodeWidth returns the dictionary code width, or zero for raw fallback.
func (column AdaptiveLowCardinalityStringColumn) CodeWidth() int {
	if !column.dictionaryMode {
		return 0
	}
	return column.dictionary.CodeWidth()
}

// DictionaryValues returns a copy of the sorted dictionary, or nil for raw
// fallback columns.
func (column AdaptiveLowCardinalityStringColumn) DictionaryValues() []string {
	if !column.dictionaryMode {
		return nil
	}
	return column.dictionary.DictionaryValues()
}

// ValueAt returns one exact row value and its SQL-style validity.
func (column AdaptiveLowCardinalityStringColumn) ValueAt(row int) (string, bool) {
	if column.dictionaryMode {
		return column.dictionary.ValueAt(row)
	}
	if row < 0 || row >= len(column.rawValues) || !adaptiveRawRowValid(column.rawValid, row) {
		return "", false
	}
	return column.rawValues[row], true
}

// LookupCode returns a sorted dictionary code only when the compact
// representation is active. Raw fallback callers should use Contains or
// ValueAt because no code map is retained.
func (column AdaptiveLowCardinalityStringColumn) LookupCode(value string) (uint32, bool) {
	if !column.dictionaryMode {
		return 0, false
	}
	return column.dictionary.LookupCode(value)
}

// Contains performs exact equality membership without changing the selected
// representation. Raw fallback uses a bounded linear scan and remains an
// allocation-free read.
func (column AdaptiveLowCardinalityStringColumn) Contains(value string) bool {
	if column.dictionaryMode {
		_, ok := column.dictionary.LookupCode(value)
		return ok
	}
	for row, candidate := range column.rawValues {
		if adaptiveRawRowValid(column.rawValid, row) && candidate == value {
			return true
		}
	}
	return false
}

// MemoryBytes estimates retained backing bytes for the selected encoding.
func (column AdaptiveLowCardinalityStringColumn) MemoryBytes() int {
	if column.dictionaryMode {
		return column.dictionary.MemoryBytes()
	}
	bytes := len(column.rawValid) * 8
	for _, value := range column.rawValues {
		bytes += 16 + len(value)
	}
	return bytes
}

func adaptiveRawRowValid(valid []uint64, row int) bool {
	return valid == nil || row >= 0 && row>>6 < len(valid) && valid[row>>6]&(uint64(1)<<uint(row&63)) != 0
}

func adaptiveRawCardinality(values []string, valid []uint64) int {
	if len(values) == 0 {
		return 0
	}
	seen := make(map[string]struct{}, len(values))
	for row, value := range values {
		if adaptiveRawRowValid(valid, row) {
			seen[value] = struct{}{}
		}
	}
	return len(seen)
}

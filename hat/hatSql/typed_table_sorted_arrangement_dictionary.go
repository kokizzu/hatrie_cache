package hatSql

// typedTableSortedArrangementStringDictionary owns one backing string for
// each live distinct sort value. Row values keep the public TypedTableValue
// shape but point at the dictionary's immutable string storage.
type typedTableSortedArrangementStringDictionary struct {
	positions map[string]uint32
	values    []string
	counts    []uint32
	free      []uint32
}

func (dictionary *typedTableSortedArrangementStringDictionary) retain(value string) string {
	if dictionary.positions == nil {
		dictionary.positions = make(map[string]uint32)
	}
	if code, found := dictionary.positions[value]; found {
		index := int(code - 1)
		dictionary.counts[index]++
		return dictionary.values[index]
	}
	var code uint32
	if length := len(dictionary.free); length > 0 {
		code = dictionary.free[length-1]
		dictionary.free = dictionary.free[:length-1]
		index := int(code - 1)
		dictionary.values[index] = value
		dictionary.counts[index] = 1
		 dictionary.positions[value] = code
		return dictionary.values[index]
	}
	code = uint32(len(dictionary.values) + 1)
	dictionary.values = append(dictionary.values, value)
	dictionary.counts = append(dictionary.counts, 1)
	dictionary.positions[value] = code
	return value
}

func (dictionary *typedTableSortedArrangementStringDictionary) release(value string) {
	if dictionary == nil || dictionary.positions == nil {
		return
	}
	code, found := dictionary.positions[value]
	if !found || code == 0 || int(code) > len(dictionary.values) {
		return
	}
	index := int(code - 1)
	if dictionary.counts[index] == 0 {
		return
	}
	dictionary.counts[index]--
	if dictionary.counts[index] != 0 {
		return
	}
	delete(dictionary.positions, value)
	dictionary.values[index] = ""
	if index == len(dictionary.values)-1 {
		for len(dictionary.values) > 0 && dictionary.counts[len(dictionary.counts)-1] == 0 {
			dictionary.values = dictionary.values[:len(dictionary.values)-1]
			dictionary.counts = dictionary.counts[:len(dictionary.counts)-1]
		}
		free := dictionary.free[:0]
		for _, freeCode := range dictionary.free {
			if int(freeCode) <= len(dictionary.values) {
				free = append(free, freeCode)
			}
		}
		dictionary.free = free
		return
	}
	dictionary.free = append(dictionary.free, code)
}

func (arrangement *TypedTableSortedArrangement) storeValues(values []TypedTableValue) []TypedTableValue {
	cloned := cloneTypedTableValues(values)
	if arrangement == nil {
		return cloned
	}
	for orderIndex, orderField := range arrangement.orderFields {
		if !orderField.dictionaryEncoded || orderField.kind != TypedTableString || orderField.index < 0 || orderField.index >= len(cloned) {
			continue
		}
		value := cloned[orderField.index]
		if !value.Valid || value.Kind != TypedTableString {
			continue
		}
		dictionary := arrangement.dictionaries[orderIndex]
		if dictionary == nil {
			dictionary = &typedTableSortedArrangementStringDictionary{}
			arrangement.dictionaries[orderIndex] = dictionary
			if orderIndex == 0 {
				arrangement.dictionary = dictionary
			}
		}
		cloned[orderField.index].String = dictionary.retain(value.String)
	}
	return cloned
}

func (arrangement *TypedTableSortedArrangement) releaseValues(values []TypedTableValue) {
	if arrangement == nil {
		return
	}
	for orderIndex, orderField := range arrangement.orderFields {
		if !orderField.dictionaryEncoded || orderField.kind != TypedTableString || orderField.index < 0 || orderField.index >= len(values) {
			continue
		}
		dictionary := arrangement.dictionaries[orderIndex]
		if dictionary == nil {
			continue
		}
		value := values[orderField.index]
		if value.Valid && value.Kind == TypedTableString {
			dictionary.release(value.String)
		}
	}
}

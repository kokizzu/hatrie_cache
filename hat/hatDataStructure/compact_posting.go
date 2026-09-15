package hatDataStructure

import "sort"

// u64PostingList keeps the first ID in the map value and allocates overflow
// storage only when a key receives a second ID. A map lookup determines
// whether the zero value is absent, so ID zero remains valid.
type u64PostingList struct {
	first uint64
	rest  *u64PostingOverflow
}

type u64PostingOverflow struct {
	first uint64
	rest  []uint64
}

func newU64PostingList(id uint64) u64PostingList {
	return u64PostingList{first: id}
}

func (list u64PostingList) appendInOrder(id uint64) u64PostingList {
	if list.rest == nil {
		list.rest = &u64PostingOverflow{first: id}
		return list
	}
	list.rest.rest = append(list.rest.rest, id)
	return list
}

func (list u64PostingList) insertSorted(id uint64) u64PostingList {
	if list.rest == nil {
		if id == list.first {
			return list
		}
		if id < list.first {
			return u64PostingList{first: id, rest: &u64PostingOverflow{first: list.first}}
		}
		return u64PostingList{first: list.first, rest: &u64PostingOverflow{first: id}}
	}
	if id == list.first {
		return list
	}
	if id < list.first {
		list.rest.rest = append(list.rest.rest, 0)
		copy(list.rest.rest[1:], list.rest.rest)
		list.rest.rest[0] = list.rest.first
		list.rest.first = list.first
		list.first = id
		return list
	}
	if id == list.rest.first {
		return list
	}
	if id < list.rest.first {
		list.rest.rest = append(list.rest.rest, 0)
		copy(list.rest.rest[1:], list.rest.rest)
		list.rest.rest[0] = list.rest.first
		list.rest.first = id
		return list
	}
	position := sort.Search(len(list.rest.rest), func(position int) bool {
		return list.rest.rest[position] >= id
	})
	if position < len(list.rest.rest) && list.rest.rest[position] == id {
		return list
	}
	list.rest.rest = append(list.rest.rest, 0)
	copy(list.rest.rest[position+1:], list.rest.rest[position:])
	list.rest.rest[position] = id
	return list
}

func (list u64PostingList) values(dst []uint64) []uint64 {
	dst = append(dst, list.first)
	if list.rest == nil {
		return dst
	}
	dst = append(dst, list.rest.first)
	return append(dst, list.rest.rest...)
}

func (list u64PostingList) removeInOrder(id uint64) (u64PostingList, bool, bool) {
	if list.rest == nil {
		if list.first != id {
			return list, false, false
		}
		return u64PostingList{}, true, true
	}
	if list.first == id {
		return list.removeAt(0)
	}
	if list.rest.first == id {
		return list.removeAt(1)
	}
	for position, candidate := range list.rest.rest {
		if candidate == id {
			return list.removeAt(position + 2)
		}
	}
	return list, false, false
}

func (list u64PostingList) removeSorted(id uint64) (u64PostingList, bool, bool) {
	if list.rest == nil {
		if list.first != id {
			return list, false, false
		}
		return u64PostingList{}, true, true
	}
	if id == list.first {
		return list.removeAt(0)
	}
	if id == list.rest.first {
		return list.removeAt(1)
	}
	position := sort.Search(len(list.rest.rest), func(position int) bool {
		return list.rest.rest[position] >= id
	})
	if position >= len(list.rest.rest) || list.rest.rest[position] != id {
		return list, false, false
	}
	return list.removeAt(position + 2)
}

func (list u64PostingList) removeAt(position int) (u64PostingList, bool, bool) {
	if position == 0 {
		if list.rest == nil {
			return u64PostingList{}, true, true
		}
		list.first = list.rest.first
		if len(list.rest.rest) == 0 {
			list.rest = nil
			return list, true, false
		}
		list.rest.first = list.rest.rest[0]
		list.rest.rest = list.rest.rest[1:]
		return list, true, false
	}
	if position == 1 {
		if len(list.rest.rest) == 0 {
			list.rest = nil
			return list, true, false
		}
		list.rest.first = list.rest.rest[0]
		list.rest.rest = list.rest.rest[1:]
		return list, true, false
	}
	restPosition := position - 2
	copy(list.rest.rest[restPosition:], list.rest.rest[restPosition+1:])
	list.rest.rest[len(list.rest.rest)-1] = 0
	list.rest.rest = list.rest.rest[:len(list.rest.rest)-1]
	return list, true, false
}

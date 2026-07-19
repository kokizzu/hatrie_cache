package hatriecache

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"
)

var (
	replicationOutboxBinaryJobMagic         = []byte{'H', 'C', 'O', 'J', 1}
	replicationOutboxBinaryDeadLettersMagic = []byte{'H', 'C', 'O', 'D', 1}
	replicationOutboxBinaryDeadSeqMagic     = []byte{'H', 'C', 'O', 'S', 1}
)

func marshalReplicationOutboxJobBinary(record replicationOutboxJob) ([]byte, error) {
	result, err := json.Marshal(record.Result)
	if err != nil {
		return nil, err
	}
	writer := newBinaryFieldWriter(replicationOutboxBinaryJobMagic, len(result)+128)
	writer.writeUvarint(record.ID)
	writer.writeBytes(result)
	writer.writeBool(!record.EnqueuedAt.IsZero())
	if !record.EnqueuedAt.IsZero() {
		writer.writeVarint(record.EnqueuedAt.UnixNano())
	}
	writer.writeUvarint(uint64(len(record.Tasks)))
	for _, task := range record.Tasks {
		writer.writeString(task.Target.ID)
		writer.writeString(task.Target.Address)
		writer.writeString(task.Target.GRPCAddress)
		writer.writeString(task.Target.Role)
		if err := appendReplicationOutboxCommand(&writer, task.Payload); err != nil {
			return nil, err
		}
	}
	return writer.bytes(), nil
}

func unmarshalReplicationOutboxJob(data []byte) (replicationOutboxJob, error) {
	if !bytes.HasPrefix(data, replicationOutboxBinaryJobMagic) {
		var record replicationOutboxJob
		if err := json.Unmarshal(data, &record); err != nil {
			return replicationOutboxJob{}, err
		}
		return record, nil
	}
	reader := newBinaryFieldReader(data[len(replicationOutboxBinaryJobMagic):])
	id, err := reader.readUvarint()
	if err != nil {
		return replicationOutboxJob{}, err
	}
	resultData, err := reader.readBytes()
	if err != nil {
		return replicationOutboxJob{}, err
	}
	record := replicationOutboxJob{ID: id}
	if err := json.Unmarshal(resultData, &record.Result); err != nil {
		return replicationOutboxJob{}, err
	}
	hasEnqueuedAt, err := reader.readBool()
	if err != nil {
		return replicationOutboxJob{}, err
	}
	if hasEnqueuedAt {
		nanos, err := reader.readVarint()
		if err != nil {
			return replicationOutboxJob{}, err
		}
		record.EnqueuedAt = time.Unix(0, nanos).UTC()
	}
	taskCount, err := reader.readUvarint()
	if err != nil {
		return replicationOutboxJob{}, err
	}
	if taskCount > uint64(math.MaxInt) || taskCount > uint64(len(data)) {
		return replicationOutboxJob{}, errors.New("hatriecache: invalid replication outbox task count")
	}
	record.Tasks = make([]replicationOutboxTask, int(taskCount))
	for index := range record.Tasks {
		target := &record.Tasks[index].Target
		if target.ID, err = reader.readString(); err != nil {
			return replicationOutboxJob{}, err
		}
		if target.Address, err = reader.readString(); err != nil {
			return replicationOutboxJob{}, err
		}
		if target.GRPCAddress, err = reader.readString(); err != nil {
			return replicationOutboxJob{}, err
		}
		if target.Role, err = reader.readString(); err != nil {
			return replicationOutboxJob{}, err
		}
		payload, err := readReplicationOutboxCommand(&reader)
		if err != nil {
			return replicationOutboxJob{}, err
		}
		record.Tasks[index].Payload = payload
		record.Tasks[index].BinaryValue = record.Tasks[index].Payload.BinaryValue
	}
	if !reader.done() {
		return replicationOutboxJob{}, errors.New("hatriecache: trailing replication outbox job data")
	}
	return record, nil
}

func appendReplicationOutboxCommand(writer *binaryFieldWriter, request CacheCommandRequest) error {
	if len(request.Values) > 0 || len(request.Batch) > 0 || !replicationOutboxPairsAreStrings(request.Pairs) {
		writer.writeUvarint(1)
		data, err := json.Marshal(request)
		if err != nil {
			return err
		}
		writer.writeBytes(data)
		writer.writeBytes(request.BinaryValue)
		return nil
	}
	writer.writeUvarint(0)
	writer.writeString(request.Command)
	writer.writeString(request.Key)
	writer.writeString(request.Value)
	writer.writeString(request.Subkey)
	writeReplicationOutboxOptionalInt64(writer, request.Priority)
	writeReplicationOutboxOptionalInt64(writer, request.TTLSeconds)
	writeReplicationOutboxOptionalInt64(writer, request.UnixSeconds)
	writer.writeBytes(request.BinaryValue)
	writer.writeUvarint(uint64(len(request.Pairs)))
	for key, rawValue := range request.Pairs {
		value := rawValue.(string)
		writer.writeString(key)
		writer.writeString(value)
	}
	return nil
}

func readReplicationOutboxCommand(reader *binaryFieldReader) (CacheCommandRequest, error) {
	mode, err := reader.readUvarint()
	if err != nil {
		return CacheCommandRequest{}, err
	}
	if mode == 1 {
		data, err := reader.readBytes()
		if err != nil {
			return CacheCommandRequest{}, err
		}
		var request CacheCommandRequest
		if err := json.Unmarshal(data, &request); err != nil {
			return CacheCommandRequest{}, err
		}
		binaryValue, err := reader.readBytes()
		if err != nil {
			return CacheCommandRequest{}, err
		}
		request.BinaryValue = append([]byte(nil), binaryValue...)
		return request, nil
	}
	if mode != 0 {
		return CacheCommandRequest{}, errors.New("hatriecache: invalid replication outbox command mode")
	}
	request := CacheCommandRequest{}
	if request.Command, err = reader.readString(); err != nil {
		return CacheCommandRequest{}, err
	}
	if request.Key, err = reader.readString(); err != nil {
		return CacheCommandRequest{}, err
	}
	if request.Value, err = reader.readString(); err != nil {
		return CacheCommandRequest{}, err
	}
	if request.Subkey, err = reader.readString(); err != nil {
		return CacheCommandRequest{}, err
	}
	if request.Priority, err = readReplicationOutboxOptionalInt64(reader); err != nil {
		return CacheCommandRequest{}, err
	}
	if request.TTLSeconds, err = readReplicationOutboxOptionalInt64(reader); err != nil {
		return CacheCommandRequest{}, err
	}
	if request.UnixSeconds, err = readReplicationOutboxOptionalInt64(reader); err != nil {
		return CacheCommandRequest{}, err
	}
	binaryValue, err := reader.readBytes()
	if err != nil {
		return CacheCommandRequest{}, err
	}
	request.BinaryValue = append([]byte(nil), binaryValue...)
	pairCount, err := reader.readUvarint()
	if err != nil || pairCount > uint64(math.MaxInt) || pairCount > uint64(len(reader.data)-reader.off) {
		return CacheCommandRequest{}, errors.New("hatriecache: invalid replication outbox command pair count")
	}
	if pairCount > 0 {
		request.Pairs = make(Map, int(pairCount))
	}
	for index := uint64(0); index < pairCount; index++ {
		key, err := reader.readString()
		if err != nil {
			return CacheCommandRequest{}, err
		}
		value, err := reader.readString()
		if err != nil {
			return CacheCommandRequest{}, err
		}
		request.Pairs[key] = value
	}
	return request, nil
}

func replicationOutboxPairsAreStrings(pairs Map) bool {
	for _, value := range pairs {
		if _, ok := value.(string); !ok {
			return false
		}
	}
	return true
}

func writeReplicationOutboxOptionalInt64(writer *binaryFieldWriter, value *int64) {
	writer.writeBool(value != nil)
	if value != nil {
		writer.writeVarint(*value)
	}
}

func readReplicationOutboxOptionalInt64(reader *binaryFieldReader) (*int64, error) {
	present, err := reader.readBool()
	if err != nil || !present {
		return nil, err
	}
	value, err := reader.readVarint()
	if err != nil {
		return nil, err
	}
	return &value, nil
}

func marshalReplicationOutboxDeadLettersBinary(deadLetters []ReplicationDeadLetter) ([]byte, error) {
	writer := newBinaryFieldWriter(replicationOutboxBinaryDeadLettersMagic, len(deadLetters)*128)
	writer.writeUvarint(uint64(len(deadLetters)))
	for _, deadLetter := range deadLetters {
		data, err := json.Marshal(deadLetter)
		if err != nil {
			return nil, err
		}
		writer.writeBytes(data)
	}
	return writer.bytes(), nil
}

func unmarshalReplicationOutboxDeadLetters(data []byte) ([]ReplicationDeadLetter, error) {
	if !bytes.HasPrefix(data, replicationOutboxBinaryDeadLettersMagic) {
		var deadLetters []ReplicationDeadLetter
		if err := json.Unmarshal(data, &deadLetters); err != nil {
			return nil, err
		}
		return deadLetters, nil
	}
	reader := newBinaryFieldReader(data[len(replicationOutboxBinaryDeadLettersMagic):])
	count, err := reader.readUvarint()
	if err != nil || count > uint64(math.MaxInt) || count > uint64(len(data)) {
		return nil, errors.New("hatriecache: invalid replication outbox dead-letter count")
	}
	deadLetters := make([]ReplicationDeadLetter, int(count))
	for index := range deadLetters {
		record, err := reader.readBytes()
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(record, &deadLetters[index]); err != nil {
			return nil, err
		}
	}
	if !reader.done() {
		return nil, errors.New("hatriecache: trailing replication outbox dead-letter data")
	}
	return deadLetters, nil
}

func marshalReplicationOutboxDeadSeqBinary(sequence uint64) []byte {
	writer := newBinaryFieldWriter(replicationOutboxBinaryDeadSeqMagic, len(replicationOutboxBinaryDeadSeqMagic)+binaryFieldMaxVarintLen64)
	writer.writeUvarint(sequence)
	return writer.bytes()
}

func unmarshalReplicationOutboxDeadSeq(data []byte) (uint64, error) {
	if !bytes.HasPrefix(data, replicationOutboxBinaryDeadSeqMagic) {
		return strconv.ParseUint(string(data), 10, 64)
	}
	reader := newBinaryFieldReader(data[len(replicationOutboxBinaryDeadSeqMagic):])
	sequence, err := reader.readUvarint()
	if err != nil {
		return 0, err
	}
	if !reader.done() {
		return 0, fmt.Errorf("hatriecache: trailing replication outbox dead sequence data")
	}
	return sequence, nil
}

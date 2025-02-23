package storage

import "encoding/binary"

func EncodeIndex(rec IndexRecord, bytes []byte) {
	binary.BigEndian.PutUint64(bytes, rec.key)
	binary.BigEndian.PutUint64(bytes, rec.value)
}

func DecodeIndex(bytes []byte) IndexRecord {
	key := binary.BigEndian.Uint64(bytes[:4])
	value := binary.BigEndian.Uint64(bytes[4:])

	return IndexRecord{
		key:   key,
		value: value,
	}
}

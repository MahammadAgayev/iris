package storage

import "encoding/binary"

func serialize(record *FileRecord) []byte {
	recordBytes := make([]byte, 28+len(record.Key)+len(record.Value))
	binary.LittleEndian.PutUint64(recordBytes[0:8], record.Offset)
	binary.LittleEndian.PutUint32(recordBytes[8:12], record.KeySize)
	binary.LittleEndian.PutUint32(recordBytes[12:16], record.ValueSize)
	binary.LittleEndian.PutUint64(recordBytes[16:24], uint64(record.CreatedAt))
	copy(recordBytes[28:], record.Key)
	copy(recordBytes[28+len(record.Key):], record.Value)

	return recordBytes
}

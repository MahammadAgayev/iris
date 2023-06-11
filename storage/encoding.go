package storage

import "github.com/prometheus/prometheus/tsdb/encoding"

func EncodeIndexRecord(record IndexRecord, buffer []byte) []byte {
	var buf encoding.Encbuf
	buf.B = buffer

	buf.PutUvarint64(record.IndexKey)
	buf.PutUvarint64(record.SegmentId)
	buf.PutUvarint64(record.SegmentId)

	return buf.Get()
}

func EncodeRecord(record Record, buffer []byte) []byte {
	var buf encoding.Encbuf
	buf.B = buffer

	buf.PutUvarint64(record.Offset)
	buf.PutUvarint32(record.KeySize)
	buf.PutUvarint32(record.ValueSize)
	buf.PutVarint64(record.CreatedAt)
	buf.PutUvarintBytes(record.Key)
	buf.PutUvarintBytes(record.Value)

	return buf.Get()
}

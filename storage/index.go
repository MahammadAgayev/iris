package storage

import (
	"github.com/prometheus/prometheus/tsdb/wlog"
)

const (
	OffsetIndexSegmentExt = "index"
)

type IndexRecord struct {
	key   uint64
	value uint64
}

type Index interface {
	Add(rec IndexRecord) error
}

type OffsetIndex struct {
	file wlog.SegmentFile
	pool *BytesPool
}

func NewOffsetIndex(file wlog.SegmentFile) Index {
	ind := &OffsetIndex{
		file: file,
		pool: NewBytesPool(16), //16 bytes of offset index record
	}

	return ind
}

func (i *OffsetIndex) Add(rec IndexRecord) error {
	bytes := i.pool.GetBytes()

	EncodeIndex(rec, *bytes)

	_, err := i.file.Write(*bytes)

	if err != nil {
		return err
	}

	err = i.file.Sync()

	if err != nil {
		return err
	}

	i.pool.PutBytes(bytes)

	return nil
}

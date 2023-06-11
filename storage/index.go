package storage

import (
	"iris/config"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/tsdb/wlog"
)

type OffsetIndex struct {
	records   []IndexRecord
	ticker    *time.Ticker
	options   config.IndexOptions
	bytesPool *BytesPool
	logger    log.Logger
	wal       *wlog.WL

	lastFlushedRecord IndexRecord
}

func NewOffsetIndex(options config.IndexOptions, logger log.Logger, dir string, registerer prometheus.Registerer) (Index, error) {

	wal, err := wlog.NewSize(logger, registerer, dir, options.SegmentSize, false)

	if err != nil {
		return nil, err
	}

	bytePool := NewBytesPool()

	return &OffsetIndex{
		records:   make([]IndexRecord, 0, options.MaxMemoryIndexRecords),
		options:   options,
		logger:    logger,
		bytesPool: bytePool,
		wal:       wal,
	}, nil
}

func (i *OffsetIndex) Run() error {
	i.ticker = time.NewTicker(i.options.WriteInterval)

	go func() {
		for range i.ticker.C {
			i.timeTick()
		}
	}()

	return nil
}

func (i *OffsetIndex) Stop() error {
	i.ticker.Stop()
	return nil
}

func (i *OffsetIndex) AddRecord(record *IndexRecord) {
	i.records = append(i.records, *record)

	if len(i.records) > i.options.MaxMemoryIndexRecords {
		i.records = i.records[len(i.records)-i.options.MaxMemoryIndexRecords:]
	}
}

func (i *OffsetIndex) timeTick() {
	for range i.ticker.C {
		lastWrittenRecord := i.records[len(i.records)-1]

		if lastWrittenRecord.IndexKey == i.lastFlushedRecord.IndexKey {
			continue
		}

		err := i.flushIndex(lastWrittenRecord)
		i.lastFlushedRecord = lastWrittenRecord

		if err != nil {
			level.Error(i.logger).Log("msg", "error writing index", "err", err)
		}
	}
}

func (i *OffsetIndex) flushIndex(record IndexRecord) error {
	bytes := i.bytesPool.GetBytes()
	defer i.bytesPool.PutBytes(bytes)

	err := i.wal.Log(*bytes)

	if err != nil {
		return err
	}

	return nil
}

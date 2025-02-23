package wal

import (
	"encoding/binary"
	"hash/crc32"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	pageSize           = 32 * 1024 // 32KB
	recordHeaderSize   = 7
	DefaultSegmentSize = 1 * 1024 * 1024 * 1024
)

var (
	InvalidSegmentSize = errors.New("Invalid segment size")
	WalClosed          = errors.New("Journal closed")
	WalAlreadyClosed   = errors.New("Journal already closed")
)

var castagnoliTable = crc32.MakeTable(crc32.Castagnoli)

type page struct {
	alloc   int
	flushed int
	buf     [pageSize]byte
}

func (p *page) remaining() int {
	return pageSize - p.alloc
}

func (p *page) full() bool {
	return pageSize-p.alloc < recordHeaderSize
}

func (p *page) reset() {
	for i := range p.buf {
		p.buf[i] = 0
	}

	p.alloc = 0
	p.flushed = 0
}

func (p *page) data() []byte {
	return p.buf[p.flushed:p.alloc]
}

type Wal struct {
	logger      log.Logger
	segmentSize int
	dir         string
	metrics     *WalMetrics
	extension   string

	segment   *Segment
	page      *page
	donePages int

	mutex     sync.Mutex
	closed    bool
	workQueue chan func()
	stopc     chan chan struct{}
}

type WalMetrics struct {
	pageFlushes     prometheus.Counter
	pageCompletions prometheus.Counter
	fsyncDuration   prometheus.Histogram
	writesFailed    prometheus.Counter
}

func NewWal(logger log.Logger, registerer prometheus.Registerer, dir string, segmentSize int, extension string) (*Wal, error) {
	if segmentSize%pageSize != 0 {
		return nil, InvalidSegmentSize
	}

	wal := &Wal{
		logger:      logger,
		segmentSize: segmentSize,
		dir:         dir,
		stopc:       make(chan chan struct{}),
		workQueue:   make(chan func(), 100),
		page:        &page{},
		extension:   extension,
	}

	wal.metrics = NewWalMetrics(prometheus.WrapRegistererWithPrefix("storage_wal_", registerer))

	lastSegmentRef, err := LastSegment(dir)

	if err != nil {
		return nil, err
	}

	var segment *Segment
	writeSegmentInd := uint64(0)

	if lastSegmentRef != nil {
		//TODO
		//load the latest of add + 1 to create new segment index
		// journal.lastOffset =
		// writeSegmentInd = lastSegmentRef.index +

		writeSegmentInd = 2
	}

	segment, err = CreateSegment(dir, writeSegmentInd, extension)

	if err != nil {
		return nil, err
	}

	wal.setSegment(segment)

	go wal.run()

	return wal, nil
}

func NewWalMetrics(registerer prometheus.Registerer) *WalMetrics {
	m := &WalMetrics{}

	m.pageFlushes = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "page_flushes_total",
		Help: "Total number of page flushes.",
	})

	m.pageCompletions = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "completed_pages_total",
		Help: "Total number of completed pages.",
	})

	m.fsyncDuration = prometheus.NewSummary(prometheus.SummaryOpts{
		Name:       "fsync_duration_seconds",
		Help:       "Duration of write log fsync.",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
	})

	m.writesFailed = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "writes_failed_total",
		Help: "Total number of write log writes that failed.",
	})

	return m
}

func (j *Wal) setSegment(segment *Segment) error {
	j.segment = segment

	stat, err := segment.Stat()

	if err != nil {
		return err
	}

	j.donePages = int(stat.Size() / pageSize)

	return nil
}

func (j *Wal) pagesPerSegment() int {
	return j.segmentSize / pageSize
}

func (j *Wal) flushPage(clear bool) error {
	j.metrics.pageFlushes.Inc()

	page := j.page

	clear = clear || page.full()

	if clear {
		page.alloc = pageSize
	}

	n, err := j.segment.Write(page.data())

	if err != nil {
		page.flushed += n
		return err
	}

	page.flushed += n

	if clear {
		page.reset()
		j.donePages++
		j.metrics.pageCompletions.Inc()
	}

	return nil
}

func (j *Wal) Log(rec []byte, baseOffset uint64) error {
	j.mutex.Lock()
	defer j.mutex.Unlock()

	if err := j.log(rec, baseOffset, true); err != nil {
		j.metrics.writesFailed.Inc()
		return err
	}

	return nil
}

// First Byte of header format:
// [ 4 bits unallocated] [1 bit snappy compression flag] [ 3 bit record type ]
const (
	snappyMask  = 1 << 3
	recTypeMask = snappyMask - 1
)

type recType uint8

const (
	recPageTerm recType = 0 // Rest of page is empty.
	recFull     recType = 1 // Full record.
	recFirst    recType = 2 // First fragment of a record.
	recMiddle   recType = 3 // Middle fragments of a record.
	recLast     recType = 4 // Final fragment of a record.
)

func recTypeFromHeader(header byte) recType {
	return recType(header & recTypeMask)
}

func (j *Wal) log(rec []byte, baseOffset uint64, final bool) error {
	if j.page.full() {
		if err := j.flushPage(true); err != nil {
			return err
		}
	}

	left := j.page.remaining() - recordHeaderSize //free bytes in active page
	leftPageCount := j.pagesPerSegment() - j.donePages - 1
	left += (pageSize - recordHeaderSize) * leftPageCount //pages left for active segmet

	if len(rec) > left {
		if err := j.nextSegment(true, baseOffset); err != nil {
			return err
		}
	}

	for i := 0; i == 0 || len(rec) > 0; i++ {
		page := j.page

		var (
			pageBorder   = Min(len(rec), page.remaining()-recordHeaderSize)
			bytesFitPage = rec[:pageBorder]
			buf          = page.buf[page.alloc:]
			recType      recType
		)

		switch {
		case i == 0 && len(bytesFitPage) == len(rec):
			recType = recFull
		case len(bytesFitPage) == len(rec):
			recType = recLast
		case i == 0:
			recType = recFirst
		default:
			recType = recMiddle
		}

		buf[0] = byte(recType)
		crc := crc32.Checksum(bytesFitPage, castagnoliTable)

		binary.BigEndian.PutUint16(buf[1:], uint16(len(bytesFitPage)))
		binary.BigEndian.PutUint32(buf[3:], crc)

		copy(buf[recordHeaderSize:], bytesFitPage)

		page.alloc += len(bytesFitPage) + recordHeaderSize

		if j.page.full() {
			if err := j.flushPage(true); err != nil {
				return err
			}
		}

		rec = rec[pageBorder:]
	}

	if final && j.page.alloc > 0 {
		if err := j.flushPage(false); err != nil {
			return err
		}
	}

	return nil
}

func (j *Wal) nextSegment(async bool, offset uint64) error {
	if j.closed {
		return WalClosed
	}

	if j.page.alloc > 0 {
		if err := j.flushPage(true); err != nil {
			return err
		}
	}

	next, err := CreateSegment(j.dir, offset, j.extension)

	if err != nil {
		return err
	}

	prev := j.segment
	j.setSegment(next)

	f := func() {
		if err := j.fsync(prev); err != nil {
			level.Error(j.logger).Log("msg", "error syncing previous segment", "err", err, "segmentId", prev.i)
		}

		if err := prev.Close(); err != nil {
			level.Error(j.logger).Log("msg", "error closing previous segment", "err", err, "segmentId", prev.i)
		}
	}

	if async {
		j.workQueue <- f
	} else {
		f()
	}

	return nil
}

func (j *Wal) fsync(s *Segment) error {
	now := time.Now()
	err := s.Sync()

	j.metrics.fsyncDuration.Observe(time.Since(now).Seconds())

	return err
}

func (j *Wal) run() {
Loop:
	for {
		select {
		case f := <-j.workQueue:
			f()
		case donec := <-j.stopc:
			close(j.workQueue)
			defer close(donec)
			break Loop
		}
	}

	for f := range j.workQueue {
		f()
	}
}

func (j *Wal) Stop() error {

	j.mutex.Lock()
	defer j.mutex.Unlock()

	if j.closed {
		return WalAlreadyClosed
	}

	if j.segment == nil {
		j.closed = true
		return nil
	}

	// Flush the last page and zero out all its remaining size.
	// We must not flush an empty page as it would falsely signal
	// the segment is done if we start writing to it again after opening.
	if j.page.alloc > 0 {
		if err := j.flushPage(true); err != nil {
			return err
		}
	}

	donec := make(chan struct{})
	j.stopc <- donec
	<-donec

	if err := j.fsync(j.segment); err != nil {
		level.Error(j.logger).Log("msg", "sync previous segment", "err", err)
	}
	if err := j.segment.Close(); err != nil {
		level.Error(j.logger).Log("msg", "close previous segment", "err", err)
	}
	j.closed = true
	return nil
}

func (j *Wal) ActiveSegmentRef() SegmentRef {
	return SegmentRef{
		name:       SegmentName(j.segment.dir, j.segment.i, j.extension),
		index:      j.segment.i,
		exntension: j.extension,
	}
}

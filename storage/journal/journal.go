package journal

import (
	"encoding/binary"
	"hash/crc32"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	pageSize         = 32 * 1024 // 32KB
	recordHeaderSize = 15
)

var (
	InvalidSegmentSize = errors.New("Invalid segment size")
	JournalClosed      = errors.New("Journal closed")
)

var castagnoliTable = crc32.MakeTable(crc32.Castagnoli)

type page struct {
	alloc   int
	flushed int
	buf     []byte
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

type JournalRecord struct {
	Offset  uint64
	Content []byte
}

type Journal struct {
	logger      log.Logger
	segmentSize int
	dir         string
	metrics     *JournalMetrics

	segment    *Segment
	page       *page
	lastOffset uint64
	donePages  int

	closed    bool
	workQueue chan func()
}

type JournalMetrics struct {
	pageFlushes     prometheus.Counter
	pageCompletions prometheus.Counter
	fsyncDuration   prometheus.Histogram
}

func NewJournal(logger log.Logger, registerer prometheus.Registerer, dir string, segmentSize int) (*Journal, error) {
	if segmentSize%pageSize != 0 {
		return nil, InvalidSegmentSize
	}

	journal := &Journal{
		logger:      logger,
		segmentSize: segmentSize,
		dir:         dir,
	}

	journal.metrics = NewJournalMetrics(prometheus.WrapRegistererWithPrefix("storage_journal_", registerer))

	lastSegmentRef, err := LastSegmentRef(dir)

	if err != nil {
		return nil, err
	}

	var segment *Segment
	if lastSegmentRef == nil {
		segment, err = CreateSegment(dir, 0)

		if err != nil {
			return nil, err
		}
	}

	segment, err = LoadSegment(dir, lastSegmentRef.index)

	if err != nil {
		return nil, err
	}

	journal.setSegment(segment)

	return journal, nil
}

func NewJournalMetrics(registerer prometheus.Registerer) *JournalMetrics {
	m := &JournalMetrics{}

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

	return m
}

func (j *Journal) setSegment(segment *Segment) error {
	j.segment = segment

	stat, err := segment.Stat()

	if err != nil {
		return err
	}

	j.donePages = int(stat.Size() / pageSize)

	return nil
}

func (j *Journal) pagesPerSegment() int {
	return j.segmentSize / pageSize
}

func (j *Journal) flushPage(clear bool) error {
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

func (j *Journal) log(rec []byte, final bool) error {
	if j.page.full() {
		if err := j.flushPage(true); err != nil {
			return err
		}
	}

	left := j.page.remaining() - recordHeaderSize //free bytes in active page
	leftPageCount := j.pagesPerSegment() - j.donePages - 1
	left += (pageSize - recordHeaderSize) * leftPageCount //pages left for active segmet

	if len(rec) > left {
		if err := j.nextSegment(true, j.lastOffset+1); err != nil {
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

		j.lastOffset++
		binary.BigEndian.PutUint16(buf[1:], uint16(len(bytesFitPage)))
		binary.BigEndian.PutUint32(buf[3:], crc)
		binary.BigEndian.PutUint64(buf[4:], j.lastOffset)

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

func (j *Journal) nextSegment(async bool, offset uint64) error {
	if j.closed {
		return JournalClosed
	}

	if j.page.alloc > 0 {
		if err := j.flushPage(true); err != nil {
			return err
		}
	}

	next, err := CreateSegment(j.dir, offset)

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

func (j *Journal) fsync(s *Segment) error {
	now := time.Now()
	err := s.Sync()

	j.metrics.fsyncDuration.Observe(time.Since(now).Seconds())

	return err
}

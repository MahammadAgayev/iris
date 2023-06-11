package wal

import "log"

const (
	pageSize         = 32 * 1024 // 32KB
	recordHeaderSize = 7
)

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

type JournalRecord struct {
	Offset  uint64
	Content []byte
}

type Journal struct {
	logger      log.Logger
	segmentSize int

	activeSegment *Segment
	activePage    *page
	lastOffset    uint64
}

package wal

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/prometheus/prometheus/tsdb/wlog"
)

type Segment struct {
	wlog.SegmentFile
	dir string
	i   uint64
}

type SegmentRef struct {
	name  string
	index uint64
}

func CreateSegment(dir string, i uint64) (*Segment, error) {
	f, err := os.OpenFile(SegmentName(dir, i), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o666)

	if err != nil {
		return nil, err
	}

	return &Segment{
		SegmentFile: f,
		dir:         dir,
		i:           i,
	}, nil
}

func SegmentName(dir string, i uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%020d", i))
}

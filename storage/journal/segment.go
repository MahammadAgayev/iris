package journal

import (
	"fmt"
	"iris/storage"
	"os"
	"path/filepath"
	"sort"
	"strconv"

	"github.com/pkg/errors"
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

func OpenReadSegment(fn string) (*Segment, error) {
	k, err := strconv.ParseUint(filepath.Base(fn), 10, 64)
	if err != nil {
		return nil, errors.New("not a valid filename")
	}
	f, err := os.Open(fn)
	if err != nil {
		return nil, err
	}
	return &Segment{SegmentFile: f, i: k, dir: filepath.Dir(fn)}, nil
}

func SegmentName(dir string, i uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%020d", i))
}

func LastSegment(dir string) (*SegmentRef, error) {
	refs, err := Segments(dir)

	if err != nil {
		return nil, err
	}

	if len(refs) == 0 {
		return nil, nil
	} else if len(refs) == 1 {
		return &refs[0], nil
	}

	sort.Slice(refs, func(i, j int) bool {
		return refs[i].index < refs[j].index
	})

	return &refs[len(refs)-1], nil
}

func Segments(dir string) ([]SegmentRef, error) {
	files, err := os.ReadDir(dir)

	if err != nil {
		return nil, err
	}

	refs := make([]SegmentRef, 0, len(files))

	for _, file := range files {

		fileName := file.Name()
		fileNameWithoutExtension := storage.FileNameWithoutExtension(fileName)

		i, err := strconv.ParseUint(fileNameWithoutExtension, 10, 64)

		if err != nil {
			return nil, errors.Wrap(err, "unable to list segments")
		}

		segmentRef := SegmentRef{
			name:  fileName,
			index: i,
		}

		refs = append(refs, segmentRef)
	}

	return refs, nil
}

func Min(l1 int, l2 int) int {
	if l1 < l2 {
		return l1
	}

	return l2
}

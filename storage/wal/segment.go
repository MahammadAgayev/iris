package wal

import (
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
    "os"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/tsdb/wlog"
)

type Segment struct {
	wlog.SegmentFile
	dir string
	i   uint64
}

type SegmentRef struct {
	name       string
	index      uint64
	extension string
}

//TODO: logs and metrics, AI also can do it

func CreateSegment(dir string, i uint64, extension string) (*Segment, error) {
	f, err := os.OpenFile(ToSegmentName(dir, i, extension), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o666)

	if err != nil {
		return nil, err
	}

	return &Segment{
		SegmentFile: f,
		dir:         dir,
		i:           i,
	}, nil
}

func OpenReadSegment(dir string, i uint64, extension string) (*Segment, error) {
	f, err := os.Open(ToSegmentName(dir, i, extension))
	if err != nil {
		return nil, err
	}
	return &Segment{SegmentFile: f, i: i, dir: dir}, nil
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

        ref, err := ToSegmentRef(file.Name())

		if err != nil {
			return nil, errors.Wrap(err, "unable to list segments")
		}

		refs = append(refs, ref)
	}

	return refs, nil
}

// Generates segment ref for the given filename
func ToSegmentRef(fn string) (SegmentRef, error) {
    fileName := filepath.Base(fn)

    ext := filepath.Ext(fileName)
    fileNameWithoutExtension := fileName[:len(fileName)-len(ext)]
    i, err := strconv.ParseUint(fileNameWithoutExtension, 10, 64)

    if err != nil {
        return SegmentRef{}, errors.Wrap(err, "unable convert to segment ref")
    }

    return SegmentRef{
        name: fileName,
        index: i,
        extension: ext[1:], // remove the dot
    }, nil
}

// Generates filename using segment info,
func ToSegmentName(dir string, i uint64, extension string) string {
	return fmt.Sprintf("%s.%s", filepath.Join(dir, fmt.Sprintf("%020d", i)), extension)
}


func Min(l1 int, l2 int) int {
	if l1 < l2 {
		return l1
	}

	return l2
}

func FileNameWithoutExtension(fileName string) string {
	return fileName[:len(fileName)-len(filepath.Ext(fileName))]
}

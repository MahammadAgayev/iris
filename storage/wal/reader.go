package wal

import (
	"encoding/binary"
	"hash/crc32"
	"io"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/tsdb/wlog"
)

type Reader struct {
	reader     io.Reader
	err        error
	rec        []byte
	buf        [pageSize]byte
	total      uint64
	curRecType recType
}

func NewReader(reader io.Reader) *Reader {
	return &Reader{reader: reader}
}

func (r *Reader) Next() bool {
	err := r.next()

	if errors.Is(err, io.EOF) {
		if r.curRecType == recFirst || r.curRecType == recMiddle {
			r.err = errors.New("last record is torn")
		}

		return false
	}

	r.err = err

	return err == nil
}

func (r *Reader) next() error {
	hdr := r.buf[:recordHeaderSize]
	buf := r.buf[recordHeaderSize:]

	r.rec = r.rec[:0]

	i := 0
	for {
		if _, err := io.ReadFull(r.reader, hdr[:1]); err != nil {
			return errors.Wrap(err, "error reading first header byte")
		}

		r.total++
		r.curRecType = recTypeFromHeader(hdr[0])

		if r.curRecType == recPageTerm {
			buf = r.buf[1:]

			k := pageSize - (r.total % pageSize)

			if k == pageSize {
				continue
			}

			n, err := io.ReadFull(r.reader, r.buf[:k])

			if err != nil {
				return errors.Wrap(err, "error reading zero bytes")
			}

			r.total += uint64(n)

			for _, c := range buf[:k] {
				if c != 0 {
					return errors.New("unexpected non-zero byte in padded page")
				}
			}
			continue
		}

		n, err := io.ReadFull(r.reader, hdr[1:])

		if err != nil {
			return errors.Wrap(err, "read remaining header")
		}

		r.total += uint64(n)

		var (
			length = binary.BigEndian.Uint16(hdr[1:])
			crc    = binary.BigEndian.Uint32(hdr[3:])
		)

		if length > pageSize-recordHeaderSize {
			return errors.Errorf("invalid record size %d", length)
		}

		n, err = io.ReadFull(r.reader, buf[:length])
		if err != nil {
			return err
		}

		r.total += uint64(n)

		if n != int(length) {
			return errors.Errorf("invalid size: expected %d, got %d", length, n)
		}

		if c := crc32.Checksum(buf[:length], castagnoliTable); c != crc {
			return errors.Errorf("invalid checksum: expected %d, got %d", crc, c)
		}

		r.rec = append(r.rec, r.buf[:length]...)

		if err := validateRecord(r.curRecType, i); err != nil {
			return err
		}

		if r.curRecType == recFull || r.curRecType == recLast {
			return nil
		}

		i++
	}
}

func validateRecord(typ recType, i int) error {
	switch typ {
	case recFull:
		if i != 0 {
			return errors.New("unexpected full record")
		}
		return nil
	case recFirst:
		if i != 0 {
			return errors.New("unexpected first record, dropping buffer")
		}
		return nil
	case recMiddle:
		if i == 0 {
			return errors.New("unexpected middle record, dropping buffer")
		}
		return nil
	case recLast:
		if i == 0 {
			return errors.New("unexpected last record, dropping buffer")
		}
		return nil
	default:
		return errors.Errorf("unexpected record type %d", typ)
	}
}

func (r *Reader) Err() error {
	if r.err == nil {
		return nil
	}

	return &wlog.CorruptionErr{
		Err:     r.err,
		Segment: -1,
		Offset:  int64(r.total),
	}
}

package journal

import (
	"encoding/binary"
	"io"

	"google.golang.org/protobuf/internal/errors"
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
			offset = binary.BigEndian.Uint64(hdr[3:])
			crc    = binary.BigEndian.Uint32(hdr[11:])
		)

		if length > pageSize-recordHeaderSize {
			return errors.Errorf("invalid record size %d", length)
		}

		n, err = io.ReadFull(r.reader, buf[:length])
		if err != nil {
			return err
		}

		r.total += uint64(n)

	}
}

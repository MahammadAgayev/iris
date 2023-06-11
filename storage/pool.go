package storage

import "sync"

type BytesPool struct {
	pool sync.Pool
}

func NewBytesPool() *BytesPool {
	return &BytesPool{
		pool: sync.Pool{
			New: func() any {
				buf := new([]byte)            // Attempt to force allocation on heap.
				*buf = make([]byte, 0, 1<<10) // 1kb
				return buf
			},
		},
	}
}

func (p *BytesPool) GetBytes() *[]byte {
	return p.pool.Get().(*[]byte)
}

func (p *BytesPool) PutBytes(b *[]byte) {
	*b = (*b)[:0]

	p.pool.Put(b)
}

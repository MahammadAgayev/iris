package storage

type Record struct {
	Offset    uint64 //8byte
	KeySize   uint32 //4byte
	ValueSize uint32 //4byte
	CreatedAt int64  //8byte
	Key       []byte
	Value     []byte
}

type RecordRequest struct {
	Key   []byte
	Value []byte
}

type IndexRecord struct {
	IndexKey  uint64 //8 byte
	SegmentId uint64
	Position  uint64
}

type Index interface {
	Run() error
	Stop() error
	AddRecord(record *IndexRecord)
}

package storage

const recordHeaderLen = 28 //considering 4 bytes buffer after hedaer
type FileRecord struct {
	Offset    uint64 //8byte
	KeySize   uint32 //4byte
	ValueSize uint32 //4byte
	CreatedAt int64  //8byte
	Key       []byte
	Value     []byte
}

type Record struct {
	Key       []byte
	Value     []byte
	CreatedAt int64
}

type PartitionConfiguration struct {
	MaxSegmentSize uint64
}

type SegmentWriter interface {
	Write(records []FileRecord) error
	Written() (uint64, error)
}

type SegmentWriterFactory interface {
	Create(path string) (SegmentWriter, error)
}

type PartitionWriter interface {
	Write(records []Record) (uint64, error) //returns written offset if successful
}

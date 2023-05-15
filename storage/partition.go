package storage

type FilePartitionWriter struct {
	writerFactory SegmentWriterFactory
	activeWriter  SegmentWriter
	folder        string
}

func NewFilePartitionWriter(folder string, writerFactory SegmentWriterFactory) PartitionWriter {

	// filename := fmt.Sprintf("%9d", 0) + ".log"
	// segmentPath := path.Join(folder, filename)

	// segmentWriter := writerFactory.Create(segmentPath)

	return nil
}

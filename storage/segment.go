package storage

import (
	"bufio"
	"os"
)

type FileSegmentWriter struct {
	file   *os.File
	writer *bufio.Writer
}

type FileSegmentWriterFactory struct {
}

func (s *FileSegmentWriterFactory) Create(path string) (SegmentWriter, error) {
	return NewFileSegment(path)
}

func NewFileSegment(path string) (SegmentWriter, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND, 0660)

	if err != nil {
		return nil, err
	}

	writer := bufio.NewWriter(file)

	segment := FileSegmentWriter{
		writer: writer,
		file:   file,
	}

	return &segment, nil
}

func (s *FileSegmentWriter) Write(records []FileRecord) error {

	bytesToWrite := make([]byte, recordHeaderLen*len(records))

	for _, v := range records {
		recordBytes := serialize(&v)

		bytesToWrite = append(bytesToWrite, recordBytes...)
	}

	_, err := s.writer.Write(bytesToWrite)

	if err != nil {
		return err
	}

	//this method should ensure that bytes are written to file
	s.writer.Flush()

	return nil
}

func (s *FileSegmentWriter) Written() (uint64, error) {

	info, err := s.file.Stat()

	if err != nil {
		return 0, err
	}

	return uint64(info.Size()), nil
}

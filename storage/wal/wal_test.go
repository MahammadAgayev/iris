package wal

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWalCreation(t *testing.T) {
	dir, err := os.MkdirTemp("", "wal_test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	logger := log.NewNopLogger()
	registry := prometheus.NewRegistry()
	extension := "wal"

	w, err := NewWal(logger, registry, dir, DefaultSegmentSize, extension)
	require.NoError(t, err)
	require.NotNil(t, w)

	// Check that a segment was created
	files, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Equal(t, 1, len(files))
	assert.True(t, len(files[0].Name()) > 0)

	// Clean up
	require.NoError(t, w.Stop())
}

func TestWalWriteRead(t *testing.T) {
	dir, err := os.MkdirTemp("", "wal_test")
	require.NoError(t, err)
	// defer os.RemoveAll(dir)

	logger := log.NewNopLogger()
	registry := prometheus.NewRegistry()
	extension := "wal"

	// Create WAL and write some data
	w, err := NewWal(logger, registry, dir, pageSize*4, extension) // Small segment size for testing
	require.NoError(t, err)

	testData := [][]byte{
		[]byte("test record 1"),
		[]byte("test record 2 with some longer content"),
		[]byte("test record 3"),
		bytes.Repeat([]byte("large record "), 1000), // Large record to test multiple pages
	}

	for i, data := range testData {
		err = w.Log(uint64(i), data)
		require.NoError(t, err)
	}

	// Close the WAL to ensure all data is flushed
	require.NoError(t, w.Stop())

	// Read the data back
	segmentRef, err := LastSegment(dir)
	require.NoError(t, err)
	require.NotNil(t, segmentRef)

	reader, err := OpenReadSegment(dir, segmentRef.index, segmentRef.extension)
	require.NoError(t, err)
	defer reader.Close()

	walReader := NewReader(reader)
	var readData [][]byte

	for walReader.Next() {
		record := walReader.Record()
		readData = append(readData, append([]byte{}, record...))
	}

	// Verify the data
	require.Equal(t, len(testData), len(readData), "Number of records read should match number written")
	for i, expected := range testData {
		assert.Equal(t, expected, readData[i], "Record %d content mismatch", i)
	}
}

func TestWalSegmentRotation(t *testing.T) {
	dir, err := os.MkdirTemp("", "wal_test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	logger := log.NewNopLogger()
	registry := prometheus.NewRegistry()
	extension := "wal"

	// Create WAL with very small segment size to force rotation
	w, err := NewWal(logger, registry, dir, pageSize*2, extension)
	require.NoError(t, err)

	// Write enough data to force segment rotation (more than segment size)
	largeData := bytes.Repeat([]byte("x"), pageSize)

	// Write to first segment
	err = w.Log(uint64(1), largeData)
	require.NoError(t, err)

	// Write to second segment
	err = w.Log(uint64(2), largeData)
	require.NoError(t, err)

	// Write to third segment
	err = w.Log(uint64(3), largeData)
	require.NoError(t, err)

	require.NoError(t, w.Stop())

	// Check that multiple segments were created
	files, err := os.ReadDir(dir)
	require.NoError(t, err)
	assert.True(t, len(files) >= 3, "Expected at least 3 segment files, got %d", len(files))
}

func TestWalRecovery(t *testing.T) {
	dir, err := os.MkdirTemp("", "wal_test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	logger := log.NewNopLogger()
	registry := prometheus.NewRegistry()
	extension := "wal"

	// First WAL instance
	w1, err := NewWal(logger, registry, dir, pageSize*4, extension)
	require.NoError(t, err)

	testData := [][]byte{
		[]byte("recovery test 1"),
		[]byte("recovery test 2"),
	}

	for i, data := range testData {
		err = w1.Log(uint64(i), data)
		require.NoError(t, err)
	}

	require.NoError(t, w1.Stop())

	// Second WAL instance (simulates recovery)
	w2, err := NewWal(logger, registry, dir, pageSize*4, extension)
	require.NoError(t, err)

	// Write additional data
	additionalData := []byte("recovery test 3")
	err = w2.Log(uint64(len(testData)), additionalData)
	require.NoError(t, err)

	require.NoError(t, w2.Stop())

	// Read all data back to verify both initial and post-recovery writes
	segmentRef, err := LastSegment(dir)
	require.NoError(t, err)

	reader, err := OpenReadSegment(dir,segmentRef.index, segmentRef.extension)
	require.NoError(t, err)
	defer reader.Close()

	walReader := NewReader(reader)
	var readData [][]byte

	for walReader.Next() {
		record := walReader.Record()
		readData = append(readData, append([]byte{}, record...))
	}

	// Verify data (should include both original and post-recovery data)
	allData := append(testData, additionalData)
	assert.Equal(t, len(allData), len(readData), "Number of records read should match total writes")

	// Since we may not be reading records in write order due to segment rotation,
	// just verify all expected data is present
	for _, expected := range allData {
		found := false
		for _, actual := range readData {
			if bytes.Equal(expected, actual) {
				found = true
				break
			}
		}
		assert.True(t, found, "Record not found in WAL: %s", expected)
	}
}

func TestWalReadMultipleSegments(t *testing.T) {
	dir, err := os.MkdirTemp("", "wal_test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	logger := log.NewNopLogger()
	registry := prometheus.NewRegistry()
	extension := "wal"

	// Create WAL with small segment size
	w, err := NewWal(logger, registry, dir, pageSize*2, extension)
	require.NoError(t, err)

	// Write data that spans multiple segments
	numRecords := 10
	expected := make([][]byte, numRecords)

	for i := 0; i < numRecords; i++ {
		data := []byte(fmt.Sprintf("test record %d", i))
		expected[i] = data
		err = w.Log(uint64(i), data)
		require.NoError(t, err)
	}

	require.NoError(t, w.Stop())

	// Check segments and read all data
	segments, err := Segments(dir)
	require.NoError(t, err)
	require.True(t, len(segments) > 1, "Expected multiple segments")

	var allRecords [][]byte

	// Read all segments in order
	for _, segment := range segments {
		reader, err := OpenReadSegment(dir, segment.index, segment.extension)
		require.NoError(t, err)

		walReader := NewReader(reader)
		for walReader.Next() {
			record := walReader.Record()
			allRecords = append(allRecords, append([]byte{}, record...))
		}

		reader.Close()
	}

	// Verify all records were read
	assert.Equal(t, numRecords, len(allRecords), "Should read all records from all segments")

	// Records might be in different order due to segment offsets
	// So just check that all expected records are present
	for _, exp := range expected {
		found := false
		for _, act := range allRecords {
			if bytes.Equal(exp, act) {
				found = true
				break
			}
		}
		assert.True(t, found, "Expected record not found: %s", exp)
	}
}

func TestWalCorruption(t *testing.T) {
	dir, err := os.MkdirTemp("", "wal_test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	logger := log.NewNopLogger()
	registry := prometheus.NewRegistry()
	extension := "wal"

	// Create WAL
	w, err := NewWal(logger, registry, dir, pageSize*4, extension)
	require.NoError(t, err)

	// Write some data
	testData := []byte("test data for corruption test")
	err = w.Log(uint64(1), testData)
	require.NoError(t, err)

	require.NoError(t, w.Stop())

	// Corrupt the file by writing random bytes into it
	segmentRef, err := LastSegment(dir)
	require.NoError(t, err)

	file, err := os.OpenFile(segmentRef.name, os.O_WRONLY, 0)
	require.NoError(t, err)

	// Corrupt CRC in the middle of the file
	_, err = file.WriteAt([]byte{0xFF, 0xFF, 0xFF, 0xFF}, 20)
	require.NoError(t, err)
	file.Close()

	// Try to read the corrupted file
	reader, err := OpenReadSegment(dir, segmentRef.index, segmentRef.extension)
	require.NoError(t, err)
	defer reader.Close()

	walReader := NewReader(reader)

	// This should either fail to read or return incorrect data
	// The specific behavior depends on your implementation
	if walReader.Next() {
		// If it reads, the CRC check should catch the corruption
		record := walReader.Record()
		assert.NotEqual(t, testData, record, "Corrupted data should not match original")
	}
}

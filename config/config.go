package config

import "time"

type Config struct {
}

type IndexOptions struct {
	WriteInterval         time.Duration
	SegmentSize           int
	MaxMemoryIndexRecords int
}

package storage

import "iris/storage/wal"

type Journal struct {
	wal   wal.Wal
	index []Index
}

type JournalMetrics struct {
}

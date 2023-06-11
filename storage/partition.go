package storage

import (
	"sync"

	"github.com/prometheus/prometheus/tsdb/wlog"
)

type Partition struct {
	log   wlog.WL
	index Index
	mutex sync.Mutex
}

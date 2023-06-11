package main

import (
	"iris/config"
	"iris/storage"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/tsdb/wlog"
)

func main() {
	logger := log.NewLogfmtLogger(os.Stdout)
	registerer := prometheus.NewRegistry()

	options := config.IndexOptions{
		WriteInterval:         1 * time.Second,
		SegmentSize:           wlog.DefaultSegmentSize,
		MaxMemoryIndexRecords: 100,
	}

	index, err := storage.NewOffsetIndex(options, logger, "index", registerer)

	if err != nil {
		level.Error(logger).Log("error while creating index", err)
	}

	index.Run()

	wg := &sync.WaitGroup{}

	wg.Add(1)

	done := false

	go func() {

		defer wg.Done()

		rec := storage.IndexRecord{
			IndexKey:  0,
			SegmentId: 0,
			Position:  0,
		}

		for !done {
			rec.IndexKey++
			index.AddRecord(&rec)
		}
	}()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	logger.Log("msg", "app started...")
	<-sigs

	done = true
	wg.Wait()

	logger.Log("msg", "exiting...")
}

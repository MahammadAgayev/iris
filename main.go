package main

import (
	"iris/storage/wal"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
)

func main() {
	logger := log.NewLogfmtLogger(os.Stdout)
	registerer := prometheus.NewRegistry()

	os.MkdirAll("data", 0777)
	//j, err := journal.NewJournal(logger, registerer, "data", wlog.DefaultSegmentSize)

	w, err := wal.NewWal(logger, registerer, "data", wal.DefaultSegmentSize, "ext")

	if err != nil {
		level.Error(logger).Log("err", err)
		return
	}

	done := false

	wg := sync.WaitGroup{}

	wg.Add(1)

	go func() {
		defer wg.Done()

		offset := 0

		now := time.Now()

		for !done {

			if err := w.Log([]byte("It's hello world test for journal"), uint64(offset)); err != nil {
				level.Error(logger).Log("err", err)
			}

			offset++
		}

		logger.Log("now", time.Now(), "since", time.Since(now), "offset", offset, "msg", "offsets has been written")
	}()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	logger.Log("msg", "app started...")
	<-sigs

	done = true
	wg.Wait()

	logger.Log("msg", "exiting...")
}

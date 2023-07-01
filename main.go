package main

import (
	"iris/storage/journal"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/tsdb/wlog"
)

func main() {
	logger := log.NewLogfmtLogger(os.Stdout)
	registerer := prometheus.NewRegistry()

	os.MkdirAll("data", 0777)
	j, err := journal.NewJournal(logger, registerer, "data", wlog.DefaultSegmentSize)

	if err != nil {
		level.Error(logger).Log("err", err)
		return
	}

	done := false

	wg := sync.WaitGroup{}

	wg.Add(1)

	go func() {
		defer wg.Done()

		for !done {
			if err := j.Log([]byte("It's hello world test for journal")); err != nil {
				level.Error(logger).Log("err", err)
			}

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

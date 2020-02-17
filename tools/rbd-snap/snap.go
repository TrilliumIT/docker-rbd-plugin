package main

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/TrilliumIT/docker-rbd-plugin/rbd"
	log "github.com/sirupsen/logrus"
)

func snap(prefix string, includeTS bool, patterns ...string) error {
	errs := []error{}
	errMu := &sync.Mutex{}
	appendErr := func(err error) {
		errMu.Lock()
		errs = append(errs, err)
		errMu.Unlock()
	}

	snapName := prefix
	if includeTS {
		snapName = snapName + "_" + time.Now().UTC().Format(time.RFC3339)
	}
	log := log.WithField("snapshot", snapName)

	patternWg := &sync.WaitGroup{}
	for _, pattern := range patterns {
		patternWg.Add(1)
		go func(pattern string) {
			defer patternWg.Done()
			snapWg := &sync.WaitGroup{}
			patternParts := strings.SplitN(pattern, "/", 2)
			if len(patternParts) != 2 {
				appendErr(fmt.Errorf("invalid pattern %v", pattern))
				return
			}
			poolName, pattern := patternParts[0], patternParts[1]
			log := log.WithField("pool", poolName).WithField("pattern", pattern)
			pool := rbd.GetPool(poolName)
			imgs, err := pool.Images()
			if err != nil {
				log.WithError(err).Error("error listing images")
				appendErr(fmt.Errorf("error listing images in %v: %w", pool, err))
				return
			}
			for _, img := range imgs {
				snapWg.Add(1)
				go func(img *rbd.Image) {
					defer snapWg.Done()
					log := log.WithField("image", img.Name())
					if m, err := filepath.Match(pattern, img.Name()); !m || err != nil {
						if err != nil {
							log.WithError(err).Error("error comparing image name to pattern")
							appendErr(err)
							return
						}
					}
					_, err := img.CreateConsistentSnapshot(snapName)
					if errors.Is(err, rbd.ErrNotMapped) {
						return
					}
					if err != nil {
						log.WithError(err).Error("error creating snapshot")
						appendErr(err)
						return
					}
					log.Info("snapshot complete")
				}(img)
			}
			snapWg.Wait()
		}(pattern)
	}
	patternWg.Wait()
	errStr := ""
	for _, err := range errs {
		if err != nil {
			errStr = errStr + "\n" + err.Error()
		}
	}
	if errStr != "" {
		return fmt.Errorf(errStr)
	}

	return nil
}

package main

import (
	"fmt"
	"path/filepath"
	"strings"
	"sync"

	"github.com/TrilliumIT/docker-rbd-plugin/rbd"
	log "github.com/sirupsen/logrus"
)

func loop(f func(*rbd.Image, *log.Entry) error, log *log.Entry, patterns ...string) error {
	errs := []error{}
	errMu := &sync.Mutex{}
	appendErr := func(err error) {
		errMu.Lock()
		errs = append(errs, err)
		errMu.Unlock()
	}

	patternWg := &sync.WaitGroup{}
	for _, pattern := range patterns {
		patternWg.Add(1)
		go func(pattern string) {
			defer patternWg.Done()
			snapWg := &sync.WaitGroup{}
			patternParts := strings.SplitN(pattern, "/", 2)
			if len(patternParts) != 2 {
				log.Error("invalid pattern")
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
					if m, err := filepath.Match(pattern, img.Name()); err != nil {
						log.WithError(err).Error("error comparing image name to pattern")
						appendErr(err)
						return
					} else if !m {
						log.Debug("no match")
						return
					}
					if err := f(img, log); err != nil {
						appendErr(err)
					}
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

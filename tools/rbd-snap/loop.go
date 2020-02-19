package main

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"

	"github.com/TrilliumIT/docker-rbd-plugin/rbd"
	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
)

type errCollector struct {
	mu         *sync.Mutex
	errs       []error
	ignoreErrs []error
}

func newErrCollector() *errCollector {
	return &errCollector{
		mu:   &sync.Mutex{},
		errs: []error{},
	}
}

func (ec *errCollector) add(err error) {
	if err != nil {
		ec.mu.Lock()
		defer ec.mu.Unlock()
		for _, ie := range ec.ignoreErrs {
			if errors.Is(err, ie) {
				return
			}
		}
		ec.errs = append(ec.errs, err)
	}
}

func (ec *errCollector) Error() string {
	ec.mu.Lock()
	var errStrs []string
	defer ec.mu.Unlock()
	for _, err := range ec.errs {
		if err != nil {
			errStrs = append(errStrs, err.Error())
		}
	}
	return strings.Join(errStrs, "; ")
}

func (ec *errCollector) err() error {
	ec.mu.Lock()
	if len(ec.errs) == 0 {
		ec.mu.Unlock()
		return nil
	}
	if len(ec.errs) == 1 {
		ec.mu.Unlock()
		return ec.errs[0]
	}
	ec.mu.Unlock()
	return ec
}

func (ec *errCollector) ignore(err error) {
	ec.mu.Lock()
	defer ec.mu.Unlock()
	ec.ignoreErrs = append(ec.ignoreErrs, err)
	filteredErrs := ec.errs[:0]
	for _, e := range ec.errs {
		if !errors.Is(e, err) {
			filteredErrs = append(filteredErrs, e)
		}
	}
	ec.errs = filteredErrs
}

func loopSnaps(f func(*rbd.Snapshot, *log.Entry) error, log *log.Entry, patterns ...string) error {
	imgF := func(img *rbd.Image, log *logrus.Entry) error {
		snaps, err := img.Snapshots()
		if err != nil {
			log.WithError(err).Error("error getting snapshots")
			return err
		}

		errs := newErrCollector()
		snapWg := &sync.WaitGroup{}

		for _, snap := range snaps {
			snapWg.Add(1)
			go func(snap *rbd.Snapshot) {
				defer snapWg.Done()
				log := log.WithField("snapshot", snap.Name())
				if err := f(snap, log); err != nil {
					errs.add(err)
				}
			}(snap)
		}
		snapWg.Wait()

		return errs.err()
	}
	return loopImgs(imgF, log, patterns...)
}

func loopImgs(f func(*rbd.Image, *log.Entry) error, log *log.Entry, patterns ...string) error {
	errs := newErrCollector()

	patternWg := &sync.WaitGroup{}
	for _, pattern := range patterns {
		patternWg.Add(1)
		go func(pattern string) {
			defer patternWg.Done()
			snapWg := &sync.WaitGroup{}
			patternParts := strings.SplitN(pattern, "/", 2)
			if len(patternParts) != 2 {
				log.Error("invalid pattern")
				errs.add(fmt.Errorf("invalid pattern %v", pattern))
				return
			}
			poolName, pattern := patternParts[0], patternParts[1]
			log := log.WithField("pool", poolName).WithField("pattern", pattern)
			pool := rbd.GetPool(poolName)
			imgs, err := pool.Images()
			if err != nil {
				log.WithError(err).Error("error listing images")
				errs.add(fmt.Errorf("error listing images in %v: %w", pool, err))
				return
			}
			for _, img := range imgs {
				snapWg.Add(1)
				go func(img *rbd.Image) {
					defer snapWg.Done()
					log := log.WithField("image", img.Name())
					if m, err := filepath.Match(pattern, img.Name()); err != nil {
						log.WithError(err).Error("error comparing image name to pattern")
						errs.add(err)
						return
					} else if !m {
						log.Debug("no match")
						return
					}
					if err := f(img, log); err != nil {
						errs.add(err)
					}
				}(img)
			}
			snapWg.Wait()
		}(pattern)
	}
	patternWg.Wait()
	return errs.err()
}

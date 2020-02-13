package main

import (
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	rbdlib "github.com/TrilliumIT/docker-rbd-plugin/rbd"
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
	snapWg := &sync.WaitGroup{}

	for _, pattern := range patterns {
		patternParts := strings.SplitN(pattern, "/", 2)
		if len(patternParts) != 2 {
			appendErr(fmt.Errorf("invalid pattern %v", pattern))
			continue
		}
		pool, pattern := patternParts[0], patternParts[1]
		log := log.WithField("pool", pool).WithField("pattern", pattern)
		rbds, err := rbdlib.ListRBDs(pool)
		if err != nil {
			log.WithError(err).Error("error listing rbds")
			appendErr(fmt.Errorf("error listing rbds in %v: %w", pool, err))
			continue
		}
		for _, rbd := range rbds {
			snapWg.Add(1)
			go func(rbdName string) {
				log := log.WithField("rbd", rbdName)
				if err = snapRbd(prefix, includeTS, pattern, pool, rbdName); err != nil {
					log.WithError(err).Error("error snapshotting rbd")
					appendErr(err)
				}
				snapWg.Done()
			}(rbd)
		}
	}
	snapWg.Wait()
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

func snapRbd(prefix string, includeTS bool, pattern, pool, rbdName string) error {
	log := log.WithField("rbd", rbdName)
	if m, err := filepath.Match(pattern, rbdName); !m || err != nil {
		if err != nil {
			return fmt.Errorf("error comparing %v to pattern %v: %w", rbdName, pattern, err)
		}
		return nil
	}
	rbd, err := rbdlib.GetRBD(pool + "/" + rbdName)
	if err != nil {
		return fmt.Errorf("error getting rbd %v/%v: %w", pool, rbd, err)
	}
	if isMapped, err := rbd.IsMapped(); !isMapped || err != nil {
		if err != nil {
			return fmt.Errorf("error determining if %v/%v is mapped: %w", pool, rbd, err)
		}
		return nil
	}
	log.Debug("getting mounts")
	mounts, err := rbd.GetMounts()
	if err != nil {
		return fmt.Errorf("error getting mounts for %v/%v: %w", pool, rbd, err)
	}

	frozen := ""
	log.Debug("freezing mount")
	for _, mount := range mounts {
		log := log.WithField("mountpoint", mount.MountPoint)
		if err = rbdlib.FSFreeze(mount.MountPoint); err != nil {
			log.WithError(err).Error("error freezing mountpoint")
			continue
		}
		frozen = mount.MountPoint
		break
	}
	if frozen == "" {
		if len(mounts) > 0 {
			return fmt.Errorf("unable to freeze rbd %v: %w", rbdName, err)
		}
		if err := rbd.IsMountedElsewhere(""); err != nil { // checks for mounts in other namespaces
			return fmt.Errorf("rbd is mapped, but only mounted in other namespaces: %w", err)
		}
	}
	log = log.WithField("frozen_fs", frozen)
	snapName := prefix
	if includeTS {
		snapName = snapName + "_" + time.Now().UTC().Format(time.RFC3339)
	}
	log = log.WithField("snapshot_name", snapName)
	log.Debug("taking snapshot")
	if _, err = rbd.Snapshot(snapName); err != nil {
		return fmt.Errorf("failed to snapshot %v@%v: %w", rbd.RBDName(), snapName, err)
	}
	log.Debug("unfreezing mount")
	if err = rbdlib.FSUnfreeze(frozen); err != nil {
		return fmt.Errorf("failed to unfreeze %v: %w", frozen, err)
	}
	log.Info("snapshot complete")
	return nil
}

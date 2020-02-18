package main

import (
	"errors"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/TrilliumIT/docker-rbd-plugin/rbd"
	log "github.com/sirupsen/logrus"
)

var ErrNoSnapshots = errors.New("no snapshots")

func mount(prefix, mountPointDir, fileSystem string, patterns ...string) error {
	mountF := func(img *rbd.Image, log *log.Entry) error {
		mountPoint := filepath.Join(mountPointDir, img.Name())
		log = log.WithField("mountpoint", mountPoint)

		if err := img.UnmountAndUnmap(mountPoint); errors.Is(err, rbd.ErrMountedElsewhere) {
			log.WithError(err).Errorf("%v is mounted elsewhere", img.FullName())
			return err
		} else if err != nil {
			log.WithError(err).Errorf("error unmounting and unmapping %v", img.FullName())
			return err
		}

		snaps, err := img.Snapshots()
		if err != nil {
			log.WithError(err).Error("error getting snapshots")
			return err
		}

		// iterate in revese order to get most recent snapshot first
		var snap *rbd.Snapshot
		for i := len(snaps) - 1; i >= 0; i-- {
			if strings.HasPrefix(snaps[i].Name(), prefix) {
				if snap == nil {
					snap = snaps[i]
					continue
				}
				if err = snaps[i].UnmountAndUnmap(mountPoint); errors.Is(err, rbd.ErrMountedElsewhere) {
					log.WithError(err).Errorf("%v is mounted elsewhere", snaps[i].FullName())
					return err
				} else if err != nil {
					log.WithError(err).Errorf("error unmounting and unmapping %v", snaps[i].FullName())
					return err
				}
			}
		}
		if snap == nil {
			log.Error("no snapshots")
			return ErrNoSnapshots
		}
		log = log.WithField("snapshot", snap.Name())

		blk, err := snap.Map()
		if err != nil {
			log.WithError(err).Error("error mapping")
			return err
		}
		log = log.WithField("blk", blk)

		flags := uintptr(syscall.MS_RDONLY)
		if fileSystem == "" {
			fileSystem, err = snap.FileSystem()
			if err != nil {
				log.WithError(err).Error("error getting filesystem")
				return err
			}
		}
		log = log.WithField("fs", fileSystem)

		mountData := ""
		if fileSystem == "xfs" {
			mountData = "norecovery"
		}

		// if already mounted, do nothing
		if mounted, err := snap.IsMountedAt(mountPoint); mounted {
			log.Debug("already mounted")
			return nil
		} else if err != nil {
			log.WithError(err).Error("error determining if mounted")
			return err
		}

		// try unmounting just in case something else is mounted there. Ignore errors
		_ = syscall.Unmount(mountPoint, 0)

		err = snap.MapAndMount(mountPoint, fileSystem, flags, mountData)
		if err != nil {
			log.WithError(err).Error("error mounting")
			return err
		}
		log.Info("mounted")
		return nil
	}

	return loopImgs(mountF, log.NewEntry(log.StandardLogger()), patterns...)
}

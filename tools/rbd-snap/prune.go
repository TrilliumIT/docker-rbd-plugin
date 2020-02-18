package main

import (
	"time"

	"github.com/TrilliumIT/docker-rbd-plugin/rbd"
	log "github.com/sirupsen/logrus"
)

func prune(prefix string, pruneAge time.Duration, pattern ...string) error {
	pruneBefore := time.Now().Add(-pruneAge)

	pruneF := func(snap *rbd.Snapshot, log *log.Entry) error {
		snapInfo, err := snap.Info()
		if err != nil {
			log.WithError(err).Error("error getting info")
			return err
		}
		if !pruneBefore.Before(time.Time(snapInfo.CreateTimestamp)) {
			return nil
		}
		if err = snap.UnmountAndUnmap(""); err != nil { // safety check
			log.WithError(err).Error("error safety unmounting")
			return err
		}
		if err = snap.Remove(); err != nil {
			log.WithError(err).Error("error removing")
			return err
		}
		return nil
	}

	return loopSnaps(pruneF, log.NewEntry(log.StandardLogger()), pattern...)
}

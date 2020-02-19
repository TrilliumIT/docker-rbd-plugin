package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/TrilliumIT/docker-rbd-plugin/rbd"
	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
)

func prune(prefix string, pruneAge time.Duration, pattern ...string) error {
	pruneBefore := time.Now().Add(-pruneAge)
	log := log.WithField("before", pruneBefore)
	log.Info("pruning snapshots")

	pruneF := func(snap *rbd.Snapshot, log *logrus.Entry) error {
		created, err := time.Parse(time.RFC3339, strings.TrimPrefix(snap.Name(), prefix+"_"))
		if err != nil {
			return fmt.Errorf("error parsing create time for %v", snap.FullName())
		}
		log = log.WithField("created", created)
		if pruneBefore.Before(created) {
			log.Debug("skipping newer snapshot")
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
		log.Info("pruned")
		return nil
	}

	return loopSnaps(pruneF, log, pattern...)
}

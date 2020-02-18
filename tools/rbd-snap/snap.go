package main

import (
	"errors"
	"time"

	"github.com/TrilliumIT/docker-rbd-plugin/rbd"
	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
)

func snap(prefix string, includeTS, onlyMapped bool, patterns ...string) error {
	snapName := prefix
	if includeTS {
		snapName = snapName + "_" + time.Now().UTC().Format(time.RFC3339)
	}
	log := log.WithField("snapshot", snapName)

	snapF := func(img *rbd.Image, log *logrus.Entry) error {
		_, err := img.CreateConsistentSnapshot(snapName, onlyMapped)
		if errors.Is(err, rbd.ErrNotMapped) {
			log.Debug("not mapped")
			return nil
		}
		if err != nil {
			log.WithError(err).Error("error creating snapshot")
			return err
		}
		log.Info("snapshot complete")
		return nil
	}

	return loop(snapF, log, patterns...)
}

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	rbdlib "github.com/TrilliumIT/docker-rbd-plugin/rbd"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli"
)

const (
	version = "0.1.0"
)

func main() {
	app := cli.NewApp()
	app.Name = "rbd-snap"
	app.Version = version
	app.Description = "take filesystem consistent snapshots of mounted rbds"
	app.ArgsUsage = "glob pattern for rbds to snapshot"

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "prefix",
			Value: "rbd-snap",
			Usage: "snapshot name prefix",
		},
		cli.BoolTFlag{
			Name:  "omit_timestamp",
			Usage: "don't add timestamp after snapshot prefix",
		},
	}
	app.Action = func(c *cli.Context) error {
		return snap(c.String("prefix"), c.BoolT("omit_timestamp"), c.Args()...)
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

func snap(prefix string, includeTS bool, patterns ...string) error {
	var err error
	for _, pattern := range patterns {
		patternParts := strings.SplitN(pattern, "/", 1)
		pool, pattern := patternParts[0], patternParts[1]
		log := log.WithField("pool", pool).WithField("pattern", pattern)
		rbds, err := rbdlib.ListRBDs(pool)
		if err != nil {
			log.WithError(err).Error("error listing rbds")
			err = fmt.Errorf("error listing rbds in %v: %w", pool, err)
			continue
		}
		for _, rbd := range rbds {
			log := log.WithField("rbd", rbd)
			if m, lErr := filepath.Match(pattern, rbd); !m || lErr != nil {
				if lErr != nil {
					log.WithError(lErr).Error("error comparing to pattern")
					err = fmt.Errorf("error comparing %v to pattern %v: %w", rbd, pattern, lErr)
				}
				continue
			}
			rbd, lErr := rbdlib.GetRBD(pool + "/" + rbd)
			if lErr != nil {
				log.WithError(lErr).Error("error getting rbd")
				err = fmt.Errorf("error getting rbd %v/%v: %w", pool, rbd, lErr)
				continue
			}
			mounts, lErr := rbd.GetMounts()
			if lErr != nil {
				log.WithError(lErr).Error("error getting mounts")
				err = fmt.Errorf("error getting mounts for %v/%v: %w", pool, rbd, lErr)
				continue
			}
			frozen := ""
			for _, mount := range mounts {
				log := log.WithField("mountpoint", mount.MountPoint)
				lErr = rbdlib.FSFreeze(mount.MountPoint)
				if lErr != nil {
					log.WithError(lErr).Error("error freezing mountpoint")
					err = fmt.Errorf("error freezing mountpoint %v: %w", mount.MountPoint, lErr)
					continue
				}
				frozen = mount.MountPoint
				break
			}
			if frozen == "" && len(mounts) > 0 {
				log.WithError(lErr).Error("unable to freeze rbd")
				continue
			}
			snapName := prefix
			if includeTS {
				snapName = snapName + "_" + time.Now().UTC().Format(time.RFC3339)
			}
			_, lErr = rbd.Snapshot(snapName)
			if lErr != nil {
				log.WithError(lErr).Errorf("failed to snapshot %v@%v", rbd.RBDName(), snapName)
				err = fmt.Errorf("failed to snapshot %v@%v: %w", rbd.RBDName(), snapName, lErr)
				continue
			}
			lErr = rbdlib.FSUnfreeze(frozen)
			if lErr != nil {
				log.WithError(lErr).Errorf("failed to unfreeze %v", frozen)
				err = fmt.Errorf("failed to unfreeze %v: %w", frozen, lErr)
				continue
			}
		}
	}

	return err
}

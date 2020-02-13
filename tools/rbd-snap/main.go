package main

import (
	"os"
	"time"

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
	app.Description = "manage filesystem consistent snapshots of rbds"
	app.ArgsUsage = "pattern of rbds to operate on"
	var prefix string
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:        "prefix",
			Value:       "rbd-snap",
			Usage:       "snapshot name prefix",
			Destination: &prefix,
		},
		cli.BoolFlag{
			Name:  "verbose",
			Usage: "verbose output",
		},
	}
	app.Before = func(c *cli.Context) error {
		if c.Bool("verbose") {
			log.SetLevel(log.DebugLevel)
		}
		return nil
	}
	var omitTimestamp bool
	var pruneAge time.Duration
	app.Commands = []cli.Command{
		{
			Name:  "snap",
			Usage: "take a snapshot of all mounted rbds",
			Flags: []cli.Flag{
				cli.BoolTFlag{
					Name:        "omit_timestamp",
					Usage:       "don't add timestamp after snapshot prefix",
					Destination: &omitTimestamp,
				},
			},
			Action: func(c *cli.Context) error {
				return snap(prefix, omitTimestamp, c.Args()...)
			},
		},
		{
			Name:  "mount",
			Usage: "mount latest snapshot for rbs",
			Action: func(c *cli.Context) error {
				return mount(prefix, c.Args()...)
			},
		},
		{
			Name:  "prune",
			Usage: "delete old snapshots",
			Flags: []cli.Flag{
				cli.DurationFlag{
					Name:        "age",
					Usage:       "keep snapshots newer than this",
					Value:       30 * 24 * time.Hour,
					Destination: &pruneAge,
				},
			},
			Action: func(c *cli.Context) error {
				return prune(prefix, pruneAge, c.Args()...)
			},
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

func mount(prefix string, pattern ...string) error {
	return nil
}

func prune(prefix string, pruneAge time.Duration, pattern ...string) error {
	return nil
}

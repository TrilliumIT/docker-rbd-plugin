package main

import (
	"os"

	"github.com/TrilliumIT/docker-rbd-plugin/rbd"
	"github.com/docker/go-plugins-helpers/volume"
	"github.com/urfave/cli"
)

const (
	version = "0.0.1"
)

func main() {
	var flagDataset = cli.StringFlag{
		Name:  "cluster-name",
		Value: "ceph",
		Usage: "Name of the ceph cluster to be used. Default \"ceph\".",
	}

	var flagDefaultSize = cli.StringFlag{
		Name:  "default-size",
		Value: "20G",
		Usage: "Default size when creating an rbd image.",
	}

	app := cli.NewApp()
	app.Name = "docker-rbd-plugin"
	app.Usage = "Docker RBD Plugin"
	app.Version = version
	app.Flags = []cli.Flag{
		flagDataset,
	}
	app.Action = Run

	//TODO: Launch a lock watcher.

	err := app.Run(os.Args)
	if err != nil {
		panic(err)
	}
}

// Run runs the driver
func Run(ctx *cli.Context) {
	d, err := rbddriver.NewRbdDriver()
	if err != nil {
		panic(err)
	}
	h := volume.NewHandler(d)
	h.ServeUnix("root", "rbd")
}

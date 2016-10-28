package main

import (
	"fmt"
	"os"
	"os/exec"

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

	var flagPool = cli.StringFlag{
		Name:  "pool",
		Value: "docker",
		Usage: "Name of the pool in which to create our rbd images. Default \"docker\". Pool MUST already exist in ceph cluster.",
	}

	var flagDefaultSize = cli.StringFlag{
		Name:  "default-size",
		Value: "20G",
		Usage: "Default size when creating an rbd image.",
	}

	var flagDefaultFS = cli.StringFlag{
		Name:  "default-fs",
		Value: "xfs",
		Usage: "Default filesystem to format for newly created rbd volumes. The corresponding `mkfs.<fs-type>` must exist on the $PATH",
	}

	app := cli.NewApp()
	app.Name = "docker-rbd-plugin"
	app.Usage = "Docker RBD Plugin"
	app.Version = version
	app.Flags = []cli.Flag{
		flagDataset,
		flagPool,
		flagDefaultSize,
		flagDefaultFS,
	}
	app.Action = Run

	//TODO: Launch a lock watching routine.

	err := app.Run(os.Args)
	if err != nil {
		panic(err)
	}
}

// Run runs the driver
func Run(ctx *cli.Context) {
	err := exec.Command("ceph", "osd", "pool", "stats", ctx.String("pool")).Run()
	if err != nil {
		panic(fmt.Sprintf("Failed to access stats for the pool: %v. Does it exist in the ceph cluster?", ctx.String("pool")))
	}

	d, err := rbddriver.NewRbdDriver(ctx.String("pool"), ctx.String("default-size"), ctx.String("default-fs"))
	if err != nil {
		panic(err)
	}
	h := volume.NewHandler(d)
	h.ServeUnix("root", "rbd")
}

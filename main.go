package main

import (
	"fmt"
	"os"
	"os/user"

	log "github.com/Sirupsen/logrus"
	"github.com/TrilliumIT/docker-rbd-plugin/rbd"
	"github.com/docker/go-plugins-helpers/volume"
	"github.com/urfave/cli"
)

const (
	version = "0.0.1"
)

func main() {

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

	app := cli.NewApp()
	app.Name = "docker-rbd-plugin"
	app.Usage = "Docker RBD Plugin"
	app.Version = version
	app.Flags = []cli.Flag{
		flagPool,
		flagDefaultSize,
	}
	app.Action = Run

	//TODO: Launch a lock watching/reaping routine.
	//TODO: re-enable any lock refresh routines we need
	//TODO: remove any open locks we don't need

	err := app.Run(os.Args)
	if err != nil {
		panic(err)
	}
}

// Run runs the driver
func Run(ctx *cli.Context) {
	u, err := user.Current()
	if err != nil {
		fmt.Println("Error trying to get the current user.")
		os.Exit(1)
	}

	if u.Uid != "0" {
		fmt.Println("Docker RBD Plugin requires root priveleges.")
		os.Exit(1)
	}

	b, err := rbddriver.PoolExists(ctx.String("pool"))
	if err != nil {
		fmt.Printf(err.Error())
		os.Exit(1)
	}

	if !b {
		fmt.Printf("The requested ceph pool %v for docker volumes does not exist in the ceph cluster.\n", ctx.String("pool"))
	}

	d, err := rbddriver.NewRbdDriver(ctx.String("pool"), ctx.String("default-size"))
	if err != nil {
		fmt.Printf(err.Error())
		os.Exit(1)
	}

	log.Debug("Launching volume handler.")
	h := volume.NewHandler(d)
	h.ServeUnix("root", "rbd")
}

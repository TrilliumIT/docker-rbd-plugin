package main

import (
	"fmt"
	"os"
	"os/signal"
	"os/user"
	"syscall"

	log "github.com/Sirupsen/logrus"
	"github.com/TrilliumIT/docker-rbd-plugin/rbd"
	"github.com/docker/go-plugins-helpers/volume"
	"github.com/urfave/cli"
)

const (
	version = "0.1.3"
)

func main() {
	fmt.Printf("Starting docker-rbd-plugin version: %v\n", version)

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
		fmt.Println("Docker RBD Plugin requires root privileges.")
		os.Exit(1)
	}

	b, err := rbddriver.PoolExists(ctx.String("pool"))
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	if !b {
		fmt.Printf("The requested ceph pool %v for docker volumes does not exist in the ceph cluster.\n", ctx.String("pool"))
	}

	d, err := rbddriver.NewRbdDriver(ctx.String("pool"), ctx.String("default-size"))
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	c := make(chan os.Signal)
	defer close(c)
	signal.Notify(c, os.Interrupt)
	signal.Notify(c, syscall.SIGTERM)
	go func() {
		<-c
		d.UnmountWait()
		os.Exit(0)
	}()

	log.Debug("Launching volume handler.")
	h := volume.NewHandler(d)
	err = h.ServeUnix("rbd", 0)
	if err != nil {
		log.WithError(err).Error("error while stopping driver")
	}
}

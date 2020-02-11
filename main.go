package main

import (
	"fmt"
	"os"
	"os/signal"
	"os/user"
	"syscall"

	"github.com/docker/go-plugins-helpers/volume"
	log "github.com/sirupsen/logrus"
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

	var flagDefaultFS = cli.StringFlag{
		Name:   "default-filesystem",
		Value:  "xfs",
		Usage:  "Default filesystem when creating an rbd image.",
		EnvVar: "RBD_DEFAULT_FS",
	}

	var flagDefaultMP = cli.StringFlag{
		Name:   "mountpoint",
		Value:  "/var/lib/docker-volumes/rbd",
		Usage:  "Mountpoint for rbd images.",
		EnvVar: "RBD_VOLUME_DIR",
	}

	app := cli.NewApp()
	app.Name = "docker-rbd-plugin"
	app.Usage = "Docker RBD Plugin"
	app.Version = version
	app.Flags = []cli.Flag{
		flagPool,
		flagDefaultSize,
		flagDefaultFS,
		flagDefaultMP,
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

	d, err := NewRbdDriver(ctx.String("pool"), ctx.String("default-size"), ctx.String("default-filesystem"), ctx.String("mountpoint"))
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
		os.Exit(0)
	}()

	log.Debug("Launching volume handler.")
	h := volume.NewHandler(d)
	err = h.ServeUnix("rbd", 0)
	if err != nil {
		log.WithError(err).Error("error while stopping driver")
	}
}

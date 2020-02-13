package main

import (
	"fmt"
	"os"
	"os/signal"
	"os/user"
	"syscall"

	"github.com/coreos/go-systemd/activation"
	"github.com/docker/go-plugins-helpers/volume"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli"
)

const (
	version = "0.1.3"
)

func main() {
	fmt.Printf("Starting docker-rbd-plugin version: %v\n", version)

	app := cli.NewApp()
	app.Name = "docker-rbd-plugin"
	app.Usage = "Docker RBD Plugin"
	app.Version = version
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "pool",
			Value: "docker",
			Usage: "Name of the pool in which to create our rbd images. Default \"docker\". Pool MUST already exist in ceph cluster.",
		},

		cli.StringFlag{
			Name:  "default-size",
			Value: "20G",
			Usage: "Default size when creating an rbd image.",
		},

		cli.StringFlag{
			Name:   "default-filesystem",
			Value:  "xfs",
			Usage:  "Default filesystem when creating an rbd image.",
			EnvVar: "RBD_DEFAULT_FS",
		},

		cli.StringFlag{
			Name:   "mountpoint",
			Value:  "/var/lib/docker-volumes/rbd",
			Usage:  "Mountpoint for rbd images.",
			EnvVar: "RBD_VOLUME_DIR",
		},
	}
	app.Action = Run

	err := app.Run(os.Args)
	if err != nil {
		panic(err)
	}
}

// Run runs the driver
func Run(ctx *cli.Context) error {
	u, err := user.Current()
	if err != nil {
		return fmt.Errorf("Error trying to get the current user.")
	}

	if u.Uid != "0" {
		return fmt.Errorf("Error trying to get the current user.")
	}

	d, err := NewRbdDriver(ctx.String("pool"), ctx.String("default-size"), ctx.String("default-filesystem"), ctx.String("mountpoint"))
	if err != nil {
		return err
	}

	c := make(chan os.Signal)
	defer close(c)
	signal.Notify(c, os.Interrupt)
	signal.Notify(c, syscall.SIGTERM)
	go func() {
		<-c
		os.Exit(0)
	}()

	h := volume.NewHandler(d)
	listeners, _ := activation.Listeners() // wtf coreos, this funciton never returns errors
	if len(listeners) == 0 {
		log.Debug("launching volume handler.")
		return h.ServeUnix("rbd", 0)
	}

	if len(listeners) > 1 {
		log.Warn("driver does not support multiple sockets")
	}

	l := listeners[0]
	log.WithField("listener", l.Addr().String()).Debug("launching volume handler")
	return h.Serve(l)
}

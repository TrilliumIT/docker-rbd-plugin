package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"os/user"
	"syscall"
	"time"

	"github.com/coreos/go-systemd/activation"
	"github.com/docker/go-plugins-helpers/volume"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli"
)

const (
	version         = "0.2.2"
	shutdownTimeout = 10 * time.Second
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
		cli.DurationFlag{
			Name:  "reap",
			Value: time.Second * 30,
			// this will scan every 5m images that are mounted in mountpoint, but nowhere else,
			// or images that are not mounted anywhere, and unmap them, if the modification time of
			// the block device is older than the duration here (to prevent unmapping something
			// an admistrator just mapped manually
			Usage: "reap mapped images (0 to disable)",
		},
	}
	app.Action = Run

	err := app.Run(os.Args)
	if err != nil {
		log.WithError(err).Fatal()
	}
}

// Run runs the driver
func Run(ctx *cli.Context) error {
	u, err := user.Current()
	if err != nil {
		return fmt.Errorf("error getting the current user")
	}

	if u.Uid != "0" {
		return fmt.Errorf("user is not root")
	}

	d, err := NewRbdDriver(ctx.String("pool"), ctx.String("default-size"), ctx.String("default-filesystem"), ctx.String("mountpoint"))
	if err != nil {
		return err
	}

	reapDur := ctx.Duration("reap")
	if reapDur != 0 {
		ticker := time.NewTicker(reapDur)
		go func() {
			for t := range ticker.C {
				d.reap(t.Add(-reapDur))
			}
		}()
	}

	h := volume.NewHandler(d)
	errCh := make(chan error)
	listeners, _ := activation.Listeners() // wtf coreos, this funciton never returns errors
	if len(listeners) > 1 {
		log.Warn("driver does not support multiple sockets")
	}
	if len(listeners) == 0 {
		log.Debug("launching volume handler on default socket")
		go func() { errCh <- h.ServeUnix("rbd", 0) }()
	} else {
		l := listeners[0]
		log.WithField("listener", l.Addr().String()).Debug("launching volume handler")
		go func() { errCh <- h.Serve(l) }()
	}

	c := make(chan os.Signal)
	defer close(c)
	signal.Notify(c, os.Interrupt)
	signal.Notify(c, syscall.SIGTERM)

	select {
	case err = <-errCh:
		log.WithError(err).Error("error running handler")
		close(errCh)
	case <-c:
	}

	toCtx, toCtxCancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer toCtxCancel()
	if sErr := h.Shutdown(toCtx); sErr != nil {
		err = sErr
		log.WithError(err).Error("error shutting down handler")
	}

	if hErr := <-errCh; hErr != nil && !errors.Is(hErr, http.ErrServerClosed) {
		err = hErr
		log.WithError(err).Error("error in handler after shutdown")
	}

	return err
}

package main

import (
	"fmt"
	"path/filepath"

	"github.com/TrilliumIT/docker-rbd-plugin/rbd"
	"github.com/docker/go-plugins-helpers/volume"
	log "github.com/sirupsen/logrus"
)

//RbdDriver implements volume.Driver
type RbdDriver struct {
	volume.Driver
	pool              string
	defaultSize       string
	defaultFileSystem string
	mountpoint        string
}

//NewRbdDriver returns a new RbdDriver
func NewRbdDriver(pool, defaultSize, defaultFileSystem, mountpoint string) (*RbdDriver, error) {
	log.SetLevel(log.DebugLevel)
	log.Debug("Creating new RbdDriver.")

	return &RbdDriver{pool: pool, defaultSize: defaultSize, defaultFileSystem: defaultFileSystem, mountpoint: mountpoint}, nil
}

func (rd *RbdDriver) getRBD(name string) (*rbd.RBD, *log.Entry, error) {
	name, log := rd.nameAndLog(name)
	rbd, err := rbd.GetRBD(name)
	if err != nil {
		log.WithError(err).Error("failed to get rbd")
		err = fmt.Errorf("error getting rbd %v: %w", name, err)
	}
	return rbd, log, err
}

func (rd *RbdDriver) nameAndLog(name string) (string, *log.Entry) {
	rbdName := rd.pool + "/" + name
	return rbdName, log.WithField("rbd", name)
}

func (rd *RbdDriver) rbdMP(rbd *rbd.RBD) string {
	return filepath.Join(rd.mountpoint, rbd.Name)
}

func (rd *RbdDriver) isMounted(rbd *rbd.RBD) (string, error) {
	mp := rd.rbdMP(rbd)
	mounted, err := rbd.IsMountedAt(mp)
	if mounted {
		return mp, err
	}
	return "", err
}

//Create creates a volume
func (rd *RbdDriver) Create(req *volume.CreateRequest) error {
	log.WithField("Request", req).Debug("create")
	name, log := rd.nameAndLog(req.Name)

	size := req.Options["size"]
	if size == "" {
		size = rd.defaultSize
	}

	rbd, err := rbd.CreateRBD(name, size)
	if err != nil {
		log.WithError(err).Error("error creating rbd")
		return fmt.Errorf("error in driver create: create: %w", err)
	}
	defer rbd.Unlock()

	fs := req.Options["fs"]
	if fs == "" {
		fs = rd.defaultFileSystem
	}

	err = rbd.MKFS(fs)
	if err != nil {
		log.WithError(err).Error("error formatting rbd")
		return fmt.Errorf("error in driver create: mkfs: %w", err)
	}

	return nil
}

//List lists the volumes
func (rd *RbdDriver) List() (*volume.ListResponse, error) {
	log.Debug("List")
	var vols []*volume.Volume

	rbds, err := rbd.ListRBDs(rd.pool)
	if err != nil {
		log.WithError(err).Error("error in driver list")
		return nil, fmt.Errorf("error in driver list for %v: %w", rd.pool, err)
	}

	for _, v := range rbds {
		vols = append(vols, &volume.Volume{Name: v})
	}

	return &volume.ListResponse{Volumes: vols}, nil
}

//Get returns a volume
func (rd *RbdDriver) Get(req *volume.GetRequest) (*volume.GetResponse, error) {
	log.WithField("Request", req).Debug("Get")

	rbd, log, err := rd.getRBD(req.Name)
	if err != nil {
		return nil, err
	}
	defer rbd.Unlock()

	vol := &volume.Volume{Name: rbd.Name}

	mp, err := rd.isMounted(rbd)
	if err != nil {
		log.WithError(err).Debug("error determining if rbd is already mounted")
	}
	if mp != "" {
		vol.Mountpoint = mp
	}

	return &volume.GetResponse{Volume: vol}, nil
}

//Remove removes a volume
func (rd *RbdDriver) Remove(req *volume.RemoveRequest) error {
	log.WithField("request", req).Debug("remove")

	rbd, log, err := rd.getRBD(req.Name)
	if err != nil {
		return err
	}
	defer rbd.Unlock()

	err = rbd.Remove()
	if err != nil {
		log.WithError(err).Error("error in driver remove")
		return fmt.Errorf("error in driver remove: %w", err)
	}

	return nil
}

//Path returns the mount point of a volume
func (rd *RbdDriver) Path(req *volume.PathRequest) (*volume.PathResponse, error) {
	log.WithField("request", req).Debug("path")

	rbd, log, err := rd.getRBD(req.Name)
	if err != nil {
		return nil, err
	}
	defer rbd.Unlock()

	mp, err := rd.isMounted(rbd)
	if err != nil {
		log.WithError(err).Error("error in driver path")
		return nil, fmt.Errorf("error in driver path: %w", err)
	}

	if mp != "" {
		return &volume.PathResponse{Mountpoint: mp}, nil
	}

	return nil, nil
}

//Mount mounts a volume
func (rd *RbdDriver) Mount(req *volume.MountRequest) (*volume.MountResponse, error) {
	log.WithField("request", req).Debug("mount")

	rbd, log, err := rd.getRBD(req.Name)
	if err != nil {
		return nil, err
	}
	defer rbd.Unlock()

	mp := rd.rbdMP(rbd)
	mp, err = rbd.Mount(mp)
	if err != nil {
		log.WithError(err).Error("error in driver mount")
		return nil, fmt.Errorf("error in driver mount: %w", err)
	}

	return &volume.MountResponse{Mountpoint: mp}, nil
}

//Unmount unmounts a volume
func (rd *RbdDriver) Unmount(req *volume.UnmountRequest) error {
	log.WithField("request", req).Debug("unmount")

	rbd, log, err := rd.getRBD(req.Name)
	if err != nil {
		return err
	}
	defer rbd.Unlock()

	mp := rd.rbdMP(rbd)
	err = rbd.UnmountAndUnmap(mp)
	if err != nil {
		log.WithError(err).Error("error in driver unmount")
		return fmt.Errorf("error in driver unmount: %w", err)
	}

	return nil
}

//Capabilities returns capabilities
func (rd *RbdDriver) Capabilities() *volume.CapabilitiesResponse {
	log.Debug("capabilities")
	return &volume.CapabilitiesResponse{Capabilities: volume.Capability{Scope: "global"}}
}

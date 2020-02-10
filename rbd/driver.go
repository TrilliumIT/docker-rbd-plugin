package rbddriver

import (
	"errors"
	"fmt"
	"path/filepath"

	"github.com/docker/go-plugins-helpers/volume"
	log "github.com/sirupsen/logrus"
)

const (
	//DrpDockerContainerDir is the default docker storage dir for container jsons
	DrpDockerContainerDir = "/var/lib/docker/containers"
	//DrpRbdBinPath is the default path of the rbd program
	DrpRbdBinPath = "/usr/bin/rbd"
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

//Create creates a volume
func (rd *RbdDriver) Create(req *volume.CreateRequest) error {
	log.WithField("Request", req).Debug("create")

	name := rd.pool + "/" + req.Name
	log := log.WithField("rbd", name)

	size := req.Options["size"]
	if size == "" {
		size = rd.defaultSize
	}

	rbd, err := CreateRBD(name, size)
	if err != nil {
		log.WithError(err).Error("error creating rbd")
		return err
	}

	_, err = rbd.Map()
	if err != nil {
		log.WithError(err).Error("error mapping rbd")
		return err
	}

	fs := req.Options["fs"]
	if fs == "" {
		fs = rd.defaultFileSystem
	}

	err = rbd.MKFS(fs)
	if err != nil {
		log.WithError(err).Error("error formatting rbd")
		return err
	}

	err = rbd.UnMap()
	if err != nil {
		log.WithError(err).Error("error unmapping rbd")
		return err
	}

	return nil
}

//List lists the volumes
func (rd *RbdDriver) List() (*volume.ListResponse, error) {
	log.Debug("List")
	var vols []*volume.Volume

	rbds, err := ListRBDs(rd.pool)
	if err != nil {
		log.WithError(err).Error("failed to list rbds")
		return nil, fmt.Errorf("failed to list rbds in %v: %w", rd.pool, err)
	}

	for _, v := range rbds {
		vols = append(vols, &volume.Volume{Name: v})
	}

	return &volume.ListResponse{Volumes: vols}, nil
}

//Get returns a volume
func (rd *RbdDriver) Get(req *volume.GetRequest) (*volume.GetResponse, error) {
	log.WithField("Request", req).Debug("Get")

	name := rd.pool + "/" + req.Name
	log := log.WithField("rbd", name)

	rbd, err := GetRBD(name)
	if err != nil {
		log.WithError(err).Error("failed to get rbd")
		return nil, fmt.Errorf("error getting rbd %v: %w", name, err)
	}

	vol := &volume.Volume{Name: rbd.Name}

	mp := filepath.Join(rd.mountpoint, rbd.Name)
	mounted, err := rbd.IsMountedAt(mp)
	if err != nil {
		log.WithError(err).Debug("error determining if rbd is already mounted")
	}
	if mounted {
		vol.Mountpoint = mp
	}

	return &volume.GetResponse{Volume: vol}, nil
}

//Remove removes a volume
func (rd *RbdDriver) Remove(req *volume.RemoveRequest) error {
	log.WithField("request", req).Debug("remove")

	name := rd.pool + "/" + req.Name
	log := log.WithField("rbd", name)

	rbd, err := GetRBD(name)
	if err != nil {
		log.WithError(err).Error("error getting rbd")
		return fmt.Errorf("error getting %v: %w", name, err)
	}

	err = rbd.Remove()
	if err != nil {
		log.WithError(err).Error("error removing rbd")
		return fmt.Errorf("error removing %v: %w", name, err)
	}

	return nil
}

//Path returns the mount point of a volume
func (rd *RbdDriver) Path(req *volume.PathRequest) (*volume.PathResponse, error) {
	log.WithField("request", req).Debug("path")

	name := rd.pool + "/" + req.Name
	log := log.WithField("rbd", name)

	rbd, err := GetRBD(name)
	if err != nil {
		log.WithError(err).Error("error getting rbd")
		return nil, fmt.Errorf("error getting %v: %w", name, err)
	}

	mp := filepath.Join(rd.mountpoint, rbd.Name)
	mounted, err := rbd.IsMountedAt(mp)
	if err != nil {
		log.WithError(err).Error("error determining if rbd is already mounted")
		return nil, fmt.Errorf("error checking if %v is mounted at %v: %w", name, mp, err)
	}

	if mounted {
		return &volume.PathResponse{Mountpoint: mp}, nil
	}

	mounts, err := rbd.GetMounts()
	if err != nil {
		log.WithError(err).Error("error getting mounts")
		return nil, fmt.Errorf("error getting mounts for %v: %w", name, err)
	}

	for _, mount := range mounts {
		return &volume.PathResponse{Mountpoint: mount.MountPoint}, nil
	}

	return nil, nil
}

//Mount mounts a volume
func (rd *RbdDriver) Mount(req *volume.MountRequest) (*volume.MountResponse, error) {
	log.WithField("request", req).Debug("mount")

	name := rd.pool + "/" + req.Name
	log := log.WithField("rbd", name)

	rbd, err := GetRBD(name)
	if err != nil {
		log.WithError(err).Error("error getting rbd")
		return nil, fmt.Errorf("error getting %v: %w", name, err)
	}

	mp := filepath.Join(rd.mountpoint, rbd.Name)
	mp, err = rbd.Mount(mp)
	if err != nil && errors.Is(err, ErrRBDNotMapped) {
		_, err = rbd.Map()
		if err != nil {
			log.WithError(err).Error("error mapping")
			return nil, fmt.Errorf("error mapping %v: %w", name, err)
		}
	}
	if err != nil {
		log.WithError(err).Error("error mounting")
		return nil, fmt.Errorf("error mounting %v to %v: %w", name, mp, err)
	}

	return &volume.MountResponse{Mountpoint: mp}, nil
}

//Unmount unmounts a volume
func (rd *RbdDriver) Unmount(req *volume.UnmountRequest) error {
	log.WithField("request", req).Debug("unmount")

	name := rd.pool + "/" + req.Name
	log := log.WithField("rbd", name)

	rbd, err := GetRBD(name)
	if err != nil {
		log.WithError(err).Error("error getting rbd")
		return fmt.Errorf("error getting %v: %w", name, err)
	}

	otherNSMounts, err := rbd.GetOtherNSMounts()
	if err != nil {
		log.WithError(err).Error("error getting other namespace mounts")
		return fmt.Errorf("error getting other ns mounts for %v: %w", name, err)
	}

	if len(otherNSMounts) == 0 {
		err = rbd.Unmount()
		if err != nil {
			log.WithError(err).Error("error unmounting")
			return fmt.Errorf("error unmounting %v: %w", name, err)
		}

		err = rbd.UnMap()
		if err != nil {
			log.WithError(err).Error("error unmapping")
			return fmt.Errorf("error unmapping %v: %w", name, err)
		}
	}

	return nil
}

//Capabilities returns capabilities
func (rd *RbdDriver) Capabilities() *volume.CapabilitiesResponse {
	log.Debug("capabilities")
	return &volume.CapabilitiesResponse{Capabilities: volume.Capability{Scope: "global"}}
}

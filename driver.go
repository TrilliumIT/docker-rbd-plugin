package main

import (
	"errors"
	"fmt"
	"path/filepath"
	"syscall"

	"github.com/TrilliumIT/docker-rbd-plugin/rbd"
	"github.com/docker/go-plugins-helpers/volume"
	log "github.com/sirupsen/logrus"
)

//RbdDriver implements volume.Driver
type RbdDriver struct {
	volume.Driver
	pool              *rbd.Pool
	defaultSize       string
	defaultFileSystem string
	mountpoint        string
}

//NewRbdDriver returns a new RbdDriver
func NewRbdDriver(pool, defaultSize, defaultFileSystem, mountpoint string) (*RbdDriver, error) {
	log.SetLevel(log.DebugLevel)
	log.Debug("Creating new RbdDriver.")

	return &RbdDriver{pool: rbd.GetPool(pool), defaultSize: defaultSize, defaultFileSystem: defaultFileSystem, mountpoint: mountpoint}, nil
}

func (rd *RbdDriver) mountPoint(img *rbd.Image) string {
	return filepath.Join(rd.mountpoint, img.Name())
}

func (rd *RbdDriver) isMounted(img *rbd.Image) (string, error) {
	mp := rd.mountPoint(img)
	mounted, err := img.IsMountedAt(mp)
	if mounted {
		return mp, err
	}
	return "", err
}

func (rd *RbdDriver) imgFullName(name string) string {
	return rd.pool.Name() + "/" + name
}

func (rd *RbdDriver) imgReqInit(name string) (string, *log.Entry, func()) {
	imgName := rd.imgFullName(name)
	lock(imgName)
	log := log.WithField("image", imgName)
	return imgName, log, func() { unlock(imgName) }
}

//Create creates a volume
func (rd *RbdDriver) Create(req *volume.CreateRequest) error {
	log.WithField("Request", req).Debug("create")

	_, log, unlock := rd.imgReqInit(req.Name)
	defer unlock()

	size := req.Options["size"]
	if size == "" {
		size = rd.defaultSize
	}

	fs := req.Options["fs"]
	if fs == "" {
		fs = rd.defaultFileSystem
	}

	_, err := rd.pool.CreateImageWithFileSystem(req.Name, size, fs, "--image-feature", "exclusive-lock")
	if err != nil {
		log.WithError(err).Error("error creating image")
		return fmt.Errorf("error in driver create: create: %w", err)
	}

	return nil
}

//List lists the volumes
func (rd *RbdDriver) List() (*volume.ListResponse, error) {
	log.Debug("List")
	log := log.WithField("pool", rd.pool.Name())

	mutexMapMutex.Lock()
	defer mutexMapMutex.Unlock()
	imgs, err := rd.pool.Images()
	if err != nil {
		log.WithError(err).Error("error in driver list")
		return nil, fmt.Errorf("error in driver list for %v: %w", rd.pool.Name(), err)
	}
	vols := make([]*volume.Volume, len(imgs), len(imgs))
	for _, img := range imgs {
		vols = append(vols, &volume.Volume{Name: img.Name()})
	}

	return &volume.ListResponse{Volumes: vols}, nil
}

func (rd *RbdDriver) getImg(name string) (*rbd.Image, error) {
	img, err := rd.pool.GetImage(name)
	if err != nil {
		log.WithField("image", rd.pool.Name()+"/"+name).WithError(err).Error("error getting device")
	}
	return img, err
}

//Get returns a volume
func (rd *RbdDriver) Get(req *volume.GetRequest) (*volume.GetResponse, error) {
	log.WithField("Request", req).Debug("Get")

	_, log, unlock := rd.imgReqInit(req.Name)
	defer unlock()

	img, err := rd.getImg(req.Name)
	if err != nil {
		return nil, fmt.Errorf("error in driver get: %w", err)
	}

	vol := &volume.Volume{Name: img.Name()}

	mp, err := rd.isMounted(img)
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

	_, log, unlock := rd.imgReqInit(req.Name)
	defer unlock()

	img, err := rd.getImg(req.Name)
	if err != nil {
		return fmt.Errorf("error in driver remove: %w", err)
	}

	if err = img.Remove(); err != nil {
		log.WithError(err).Error("error in driver remove")
		return fmt.Errorf("error in driver remove: %w", err)
	}
	return err
}

//Path returns the mount point of a volume
func (rd *RbdDriver) Path(req *volume.PathRequest) (*volume.PathResponse, error) {
	log.WithField("request", req).Debug("path")

	_, log, unlock := rd.imgReqInit(req.Name)
	defer unlock()

	img, err := rd.getImg(req.Name)
	if err != nil {
		return nil, fmt.Errorf("error in driver path: %w", err)
	}

	mp, err := rd.isMounted(img)
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

	_, log, unlock := rd.imgReqInit(req.Name)
	defer unlock()

	img, err := rd.getImg(req.Name)
	if err != nil {
		return nil, fmt.Errorf("error in driver mount: %w", err)
	}

	mp := rd.mountPoint(img)
	err = img.MapAndMountExclusive(mp, syscall.MS_NOATIME)
	if err != nil {
		log.WithError(err).Error("error in driver mount")
		return nil, fmt.Errorf("error in driver mount: %w", err)
	}

	return &volume.MountResponse{Mountpoint: mp}, nil
}

//Unmount unmounts a volume
func (rd *RbdDriver) Unmount(req *volume.UnmountRequest) error {
	log.WithField("request", req).Debug("unmount")

	_, log, unlock := rd.imgReqInit(req.Name)
	defer unlock()

	img, err := rd.getImg(req.Name)
	if err != nil {
		return fmt.Errorf("error in driver unmount: %w", err)
	}

	mp := rd.mountPoint(img)
	err = img.UnmountAndUnmap(mp)
	if err != nil {
		if errors.Is(err, rbd.ErrMountedElsewhere) {
			log.WithError(err).Info("device still in use, not unmounting")
			return nil
		}
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

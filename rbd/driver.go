package rbddriver

import (
	"fmt"
	"os"
	"sync"

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
	defaultSize string
	pool        string
	mounts      map[string]*RbdImage
	mutex       sync.Mutex
	unmountWg   sync.WaitGroup
}

//UnmountWait blocks until the wg is empty
func (rd *RbdDriver) UnmountWait() {
	rd.unmountWg.Wait()
}

//NewRbdDriver returns a new RbdDriver
func NewRbdDriver(pool, ds string) (*RbdDriver, error) {
	log.SetLevel(log.DebugLevel)
	log.Debug("Creating new RbdDriver.")

	mnts := make(map[string]*RbdImage)

	mappings, err := GetMappings(pool)
	if err != nil {
		log.Errorf(err.Error())
		return nil, fmt.Errorf("error getting initial mappings")
	}
	log.WithField("Current Mappings", mappings).Debug("currently mapped images")

	var used map[string][]*Container
	used, err = GetImagesInUse(pool)
	if err != nil {
		log.WithError(err).Error("error getting images in use")
	}
	log.WithField("Used Images", used).Debug("images detected in use")

	var img *RbdImage
	for _, m := range mappings {
		image := m["pool"] + "/" + m["name"]
		img, err = LoadRbdImage(image)
		if err != nil {
			log.WithError(err).WithField("image", image).Error("error loading image")
			continue
		}

		mnts[img.image] = img
	}

	log.WithField("StartupMnts", mnts).Debug("starting up with these mnts")

	return &RbdDriver{pool: pool, defaultSize: ds, mounts: mnts, mutex: sync.Mutex{}}, nil
}

//Create creates a volume
func (rd *RbdDriver) Create(req *volume.CreateRequest) error {
	log.WithField("Request", req).Debug("create")

	image := rd.pool + "/" + req.Name

	size := rd.defaultSize
	if s, ok := req.Options["size"]; ok {
		size = s
	}

	fs := os.Getenv("DRP_DEFAULT_FS")
	if fs == "" {
		fs = "xfs"
	}
	if ft, ok := req.Options["fstype"]; ok {
		fs = ft
	}

	_, err := CreateRbdImage(image, size, fs)
	if err != nil {
		log.Errorf(err.Error())
		msg := fmt.Errorf("error while creating image %v", image)
		return msg
	}

	return nil
}

//List lists the volumes
func (rd *RbdDriver) List() (*volume.ListResponse, error) {
	log.Debug("List")
	var vols []*volume.Volume

	imgs, err := GetImages(rd.pool)
	if err != nil {
		log.Errorf(err.Error())
		msg := fmt.Errorf("failed to get list of images for pool %v", rd.pool)
		return nil, msg
	}

	for _, v := range imgs {
		vols = append(vols, &volume.Volume{Name: v})
	}

	return &volume.ListResponse{Volumes: vols}, nil
}

//Get returns a volume
func (rd *RbdDriver) Get(req *volume.GetRequest) (*volume.GetResponse, error) {
	log.WithField("Request", req).Debug("Get")

	image := rd.pool + "/" + req.Name

	img, err := LoadRbdImage(image)
	if err != nil {
		log.Errorf(err.Error())
		msg := fmt.Errorf("error while loading image %v", image)
		return nil, msg
	}

	vol := &volume.Volume{Name: img.ShortName()}

	b, err := img.IsMounted()
	if err == nil && b {
		mp, _ := img.GetMountPoint()
		if mp != "" {
			vol.Mountpoint = mp
		}
	}

	return &volume.GetResponse{Volume: vol}, nil
}

//Remove removes a volume
func (rd *RbdDriver) Remove(req *volume.RemoveRequest) error {
	log.WithField("request", req).Debug("remove")

	image := rd.pool + "/" + req.Name

	img, err := LoadRbdImage(image)
	if err != nil {
		log.Errorf(err.Error())
		msg := fmt.Errorf("error loading image %v", req.Name)
		return msg
	}

	err = img.Remove()
	if err != nil {
		log.Errorf(err.Error())
		msg := fmt.Errorf("error occurred while removing the image %v", image)
		return msg
	}

	return nil
}

//Path returns the mount point of a volume
func (rd *RbdDriver) Path(req *volume.PathRequest) (*volume.PathResponse, error) {
	log.WithField("request", req).Debug("path")

	image := rd.pool + "/" + req.Name

	img, err := LoadRbdImage(image)
	if err != nil {
		log.Errorf(err.Error())
		msg := fmt.Errorf("error loading image %v", req.Name)
		return nil, msg
	}

	b, err := img.IsMapped()
	if err != nil {
		log.Errorf(err.Error())
		msg := fmt.Errorf("error trying to determine if image %v is mapped", image)
		return nil, msg
	}

	if !b {
		return nil, fmt.Errorf("image %v is not mapped to a device", image)
	}

	b, err = img.IsMounted()
	if err != nil {
		log.Errorf(err.Error())
		msg := fmt.Errorf("error trying to determine if image %v is mounted", image)
		return nil, msg
	}

	if !b {
		return nil, fmt.Errorf("image %v is mapped, but not mounted", image)
	}

	mp, err := img.GetMountPoint()
	if err != nil {
		log.Errorf(err.Error())
		msg := fmt.Errorf("error trying to get the mount point of image %v", image)
		return nil, msg
	}

	return &volume.PathResponse{Mountpoint: mp}, nil
}

//Mount mounts a volume
func (rd *RbdDriver) Mount(req *volume.MountRequest) (*volume.MountResponse, error) {
	log.WithField("request", req).Debug("mount")
	var err error
	var img *RbdImage

	rd.mutex.Lock()
	defer rd.mutex.Unlock()

	image := rd.pool + "/" + req.Name

	img, ok := rd.mounts[image]
	if !ok {
		img, err = LoadRbdImage(image)
		if err != nil {
			log.Errorf(err.Error())
			msg := fmt.Errorf("error loading image %v", image)
			return nil, msg
		}
	}

	mp, err := img.Mount(req.ID)
	if err != nil {
		log.Errorf(err.Error())
		msg := fmt.Errorf("error while mounting image %v", image)
		return nil, msg
	}

	rd.mounts[image] = img

	return &volume.MountResponse{Mountpoint: mp}, nil
}

//Unmount unmounts a volume
func (rd *RbdDriver) Unmount(req *volume.UnmountRequest) error {
	log.WithField("request", req).Debug("unmount")

	image := rd.pool + "/" + req.Name

	rd.mutex.Lock()
	defer rd.mutex.Unlock()
	img, ok := rd.mounts[image]
	if !ok {
		msg := fmt.Errorf("could not find image object for %v in %v", image, rd.mounts)
		return msg
	}

	rd.unmountWg.Add(1)
	defer rd.unmountWg.Done()
	err := img.Unmount(req.ID)
	if err != nil {
		log.Errorf(err.Error())
		msg := fmt.Errorf("error unmounting image %v", image)
		return msg
	}

	return nil
}

//Capabilities returns capabilities
func (rd *RbdDriver) Capabilities() *volume.CapabilitiesResponse {
	log.Debug("capabilities")
	return &volume.CapabilitiesResponse{Capabilities: volume.Capability{Scope: "global"}}
}

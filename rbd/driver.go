package rbddriver

import (
	"fmt"
	"os"
	"os/exec"

	log "github.com/Sirupsen/logrus"
	"github.com/docker/go-plugins-helpers/volume"
)

type RbdDriver struct {
	volume.Driver
	defaultSize string
	defaultFS   string
	pool        string
}

func NewRbdDriver(pool, ds, df string) (*RbdDriver, error) {
	log.SetLevel(log.DebugLevel)
	log.Debug("Creating new RbdDriver.")

	return &RbdDriver{pool: pool, defaultSize: ds, defaultFS: df}, nil
}

func (rd *RbdDriver) Create(req volume.Request) volume.Response {
	log.WithField("Request", req).Debug("Create")
	//TODO: see if size can be passed in by req.Opts
	size := rd.defaultSize
	img, err := newRbdImage(rd.pool, req.Name)
	if err != nil {
		log.Errorf(err.Error())
		msg := fmt.Sprintf("Failed to generate data structure for image %v/%v.", rd.pool, req.Name)
		log.Errorf(msg)
		return volume.Response{Err: msg}
	}

	err = img.create(size)
	if err != nil {
		log.Errorf(err.Error())
		msg := fmt.Sprintf("Failed to create image %v/%v.", rd.pool, req.Name)
		log.Errorf(msg)
		return volume.Response{Err: msg}
	}

	dev, err := img.mapDevice()
	if err != nil {
		log.Errorf(err.Error())
		msg := fmt.Sprintf("Failed to map the device from image %v.", img.fullName())
		log.Errorf(msg)
		return volume.Response{Err: msg}
	}

	err = exec.Command("mkfs."+rd.defaultFS, dev.name).Run()
	if err != nil {
		log.Errorf(err.Error())
		msg := fmt.Sprintf("Failed to create the filesystem on device %v for image %v/%v.", dev.name, rd.pool, req.Name)
		log.Errorf(msg)
		return volume.Response{Err: msg}
	}

	err = img.unmapDevice()
	if err != nil {
		log.Errorf(err.Error())
		msg := fmt.Sprintf("Failed to unmap device %v for image %v/%v after creating filesystem.", dev.name, rd.pool, req.Name)
		log.Errorf(msg)
		return volume.Response{Err: msg}
	}

	return volume.Response{Err: ""}
}

func (rd *RbdDriver) List(req volume.Request) volume.Response {
	log.WithField("Requst", req).Debug("List")
	var vols []*volume.Volume

	imgs, err := getImages(rd.pool)
	if err != nil {
		log.Errorf(err.Error())
		msg := fmt.Sprintf("Failed to get list of images for pool %v.", rd.pool)
		log.Errorf(msg)
		return volume.Response{Volumes: nil, Err: msg}
	}

	for _, v := range imgs {
		vols = append(vols, &volume.Volume{Name: v})
	}

	return volume.Response{Volumes: vols, Err: ""}
}

func (rd *RbdDriver) Get(req volume.Request) volume.Response {
	log.WithField("Request", req).Debug("Get")

	img, err := newRbdImage(rd.pool, req.Name)
	if err != nil {
		log.Errorf(err.Error())
		msg := fmt.Sprintf("Failed to generate data structure for image %v.", req.Name)
		log.Errorf(msg)
		return volume.Response{Volume: nil, Err: err.Error()}
	}

	b, err := img.exists()
	if err != nil {
		log.Errorf(err.Error())
		msg := fmt.Sprintf("Failed to determine if image %v exists.", img.fullName())
		log.Errorf(msg)
		return volume.Response{Volume: nil, Err: err.Error()}
	}

	if !b {
		return volume.Response{Volume: nil, Err: "Volume does not exist."}
	}

	return volume.Response{Volume: &volume.Volume{Name: img.name}, Err: ""}
}

func (rd *RbdDriver) Remove(req volume.Request) volume.Response {
	log.WithField("Request", req).Debug("Remove")

	img, err := newRbdImage(rd.pool, req.Name)
	if err != nil {
		log.Errorf(err.Error())
		msg := fmt.Sprintf("Failed to generate data structure for image %v.", req.Name)
		log.Errorf(msg)
		return volume.Response{Volume: nil, Err: err.Error()}
	}

	err = img.remove()
	if err != nil {
		log.Errorf(err.Error())
		msg := fmt.Sprintf("Error occurred while removing the image %v.", img.fullName())
		log.Errorf(msg)
		return volume.Response{Err: err.Error()}
	}

	return volume.Response{Err: ""}
}

func (rd *RbdDriver) Path(req volume.Request) volume.Response {
	log.WithField("Request", req).Debug("Path")

	img, err := newRbdImage(rd.pool, req.Name)
	if err != nil {
		log.Errorf(err.Error())
		msg := fmt.Sprintf("Failed to generate data structure for image %v.", req.Name)
		log.Errorf(msg)
		return volume.Response{Volume: nil, Err: msg}
	}

	b, err := img.isMapped()
	if err != nil {
		log.Errorf(err.Error())
		msg := fmt.Sprintf("Error trying to determine if image %v is mapped.", img.fullName())
		log.Errorf(msg)
		return volume.Response{Err: msg}
	}

	if !b {
		return volume.Response{Err: fmt.Sprintf("Image %v is not mapped to a device.", img.fullName())}
	}

	b, err = img.isMounted()
	if err != nil {
		log.Errorf(err.Error())
		msg := fmt.Sprintf("Error trying to determine if image %v is mounted.", img.fullName())
		log.Errorf(msg)
		return volume.Response{Err: msg}
	}

	if !b {
		return volume.Response{Err: fmt.Sprintf("Image %v is mapped, but not mounted.", img.fullName())}
	}

	mp, err := img.getMountPoint()
	if err != nil {
		log.Errorf(err.Error())
		msg := fmt.Sprintf("Error trying to get the mount point of image %v.", img.fullName())
		log.Errorf(msg)
		return volume.Response{Err: msg}
	}

	return volume.Response{Mountpoint: mp, Err: ""}
}

func (rd *RbdDriver) Mount(req volume.MountRequest) volume.Response {
	log.WithField("Request", req).Debug("Mount")
	var err error

	img, err := newRbdImage(rd.pool, req.Name)
	if err != nil {
		log.Errorf(err.Error())
		msg := fmt.Sprintf("Error generating data structure for image %v/%v.", rd.pool, req.Name)
		log.Errorf(msg)
		return volume.Response{Err: msg}
	}

	err = img.lock(req.ID)
	if err != nil {
		log.Errorf(err.Error())
		msg := fmt.Sprintf("Error acquiring lock on image %v with id %v.", img.fullName(), req.ID)
		log.Errorf(msg)
		return volume.Response{Err: msg}
	}
	defer func() {
		if err != nil {
			_ = img.unlock(req.ID)
		}
	}()

	dev, err := img.mapDevice()
	if err != nil {
		log.Errorf(err.Error())
		msg := fmt.Sprintf("Error mapping image %v to device.", img.fullName(), req.ID)
		log.Errorf(msg)
		return volume.Response{Err: msg}
	}
	defer func() {
		if err != nil {
			_ = img.unmapDevice()
		}
	}()

	mp := os.Getenv("DRB_VOLUME_DIR")
	if mp == "" {
		mp = "/var/lib/docker-volumes/rbd"
	}

	mp += "/" + img.fullName()
	err = os.MkdirAll(mp, 0755)
	if err != nil {
		log.Errorf(err.Error())
		msg := fmt.Sprintf("Error creating new mount point at %v.", mp)
		log.Errorf(msg)
		return volume.Response{Err: msg}
	}

	err = dev.mount(rd.defaultFS, mp)
	if err != nil {
		log.Errorf(err.Error())
		msg := fmt.Sprintf("Error while mounting image %v from device %v to path %v.", img.fullName(), dev.name, mp)
		log.Errorf(msg)
		return volume.Response{Err: msg}
	}

	return volume.Response{Mountpoint: mp, Err: ""}
}

func (rd *RbdDriver) Unmount(req volume.UnmountRequest) volume.Response {
	log.WithField("Request", req).Debug("Unmount")

	img, err := newRbdImage(rd.pool, req.Name)
	if err != nil {
		log.Errorf(err.Error())
		msg := fmt.Sprintf("Error generating data structure for image %v/%v.", rd.pool, req.Name)
		log.Errorf(msg)
		return volume.Response{Err: msg}
	}

	dev, err := img.getDevice()
	if err != nil {
		log.Errorf(err.Error())
		msg := fmt.Sprintf("Error getting mount point for image %v.", img.fullName())
		log.Errorf(msg)
		return volume.Response{Err: msg}
	}

	err = dev.unmount()
	if err != nil {
		log.Errorf(err.Error())
		msg := fmt.Sprintf("Error unmounting image %v from device %v.", img.fullName(), dev.name)
		log.Errorf(msg)
		return volume.Response{Err: msg}
	}

	err = img.unmapDevice()
	if err != nil {
		log.Errorf(err.Error())
		msg := fmt.Sprintf("Error unmapping image %v from device %v.", img.fullName(), dev.name)
		log.Errorf(msg)
		return volume.Response{Err: msg}
	}

	err = img.unlock(req.ID)
	if err != nil {
		log.Errorf(err.Error())
		msg := fmt.Sprintf("Error unlocking image %v with id %v.", img.fullName(), req.ID)
		log.Errorf(msg)
		return volume.Response{Err: msg}
	}

	return volume.Response{Err: ""}
}

func (rd *RbdDriver) Capabilities(req volume.Request) volume.Response {
	log.WithField("Request", req).Debug("Capabilites")
	return volume.Response{Capabilities: volume.Capability{Scope: "local"}}
}

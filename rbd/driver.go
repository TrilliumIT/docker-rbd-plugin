package rbddriver

import (
	"fmt"
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
		msg := fmt.Sprintf("Failed to generate data structure for image %v/%v.", rd.pool, req.Name)
		log.Errorf(msg)
		log.Errorf(err.Error())
		return volume.Response{Err: msg}
	}

	err = img.create(size)
	if err != nil {
		msg := fmt.Sprintf("Failed to create image %v/%v.", rd.pool, req.Name)
		log.Errorf(msg)
		log.Errorf(err.Error())
		return volume.Response{Err: msg}
	}

	dev, err := img.getDevice()
	if err != nil {
		msg := fmt.Sprintf("Failed to get the device from image %v", img.name)
		log.Errorf(msg)
		log.Errorf(err.Error())
		return volume.Response{Err: msg}
	}

	err = exec.Command("mkfs."+rd.defaultFS, dev.name).Run()
	if err != nil {
		msg := fmt.Sprintf("Failed to create the filesystem on device %v for image %v/%v.", dev.name, rd.pool, req.Name)
		log.Errorf(msg)
		log.Errorf(err.Error())
		return volume.Response{Err: msg}
	}

	err = img.unmapDevice()
	if err != nil {
		msg := fmt.Sprintf("Failed to unmap device %v for image %v/%v after creating filesystem.", dev.name, rd.pool, req.Name)
		log.Errorf(msg)
		log.Errorf(err.Error())
		return volume.Response{Err: msg}
	}

	return volume.Response{Err: ""}
}

func (rd *RbdDriver) List(req volume.Request) volume.Response {
	log.WithField("Requst", req).Debug("List")
	var vols []*volume.Volume

	imgs, err := getImages(rd.pool)
	if err != nil {
		msg := fmt.Sprintf("Failed to get list of images for pool %v.", rd.pool)
		log.Errorf(msg)
		log.Errorf(err.Error())
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
		msg := fmt.Sprintf("Failed to generate data structure for image %v.", req.Name)
		log.Errorf(msg)
		log.Errorf(err.Error())
		return volume.Response{Volume: nil, Err: err.Error()}
	}

	b, err := img.exists()
	if err != nil {
		msg := fmt.Sprintf("Failed to determine if image %v exists.", img.fullName())
		log.Errorf(msg)
		log.Errorf(err.Error())
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
		msg := fmt.Sprintf("Failed to generate data structure for image %v.", req.Name)
		log.Errorf(msg)
		log.Errorf(err.Error())
		return volume.Response{Volume: nil, Err: err.Error()}
	}

	err = img.remove()
	if err != nil {
		msg := fmt.Sprintf("Error occurred while removing the image %v.", img.fullName())
		log.Errorf(msg)
		log.Errorf(err.Error())
		return volume.Response{Err: err.Error()}
	}

	return volume.Response{Err: ""}
}

func (rd *RbdDriver) Path(req volume.Request) volume.Response {
	log.WithField("Request", req).Debug("Path")

	img, err := newRbdImage(rd.pool, req.Name)
	if err != nil {
		msg := fmt.Sprintf("Failed to generate data structure for image %v.", req.Name)
		log.Errorf(msg)
		log.Errorf(err.Error())
		return volume.Response{Volume: nil, Err: err.Error()}
	}

	mp, err := img.getMountPoint()
	if err != nil {
		msg := fmt.Sprintf("Error trying to get the mount point of image %v.", img.fullName())
		log.Errorf(msg)
		log.Errorf(err.Error())
		return volume.Response{Err: ""}
	}

	return volume.Response{Mountpoint: mp, Err: ""}
}

func (rd *RbdDriver) Mount(req volume.MountRequest) volume.Response {
	log.WithField("Request", req).Debug("Mount")

	//return rd.Path(volume.Request{Name: req.Name})
	return volume.Response{Err: "Mount is not yet implmented."}
}

func (rd *RbdDriver) Unmount(req volume.UnmountRequest) volume.Response {
	log.WithField("Request", req).Debug("Unmount")

	return volume.Response{Err: "Unmount is not yet implemented."}
}

func (rd *RbdDriver) Capabilities(req volume.Request) volume.Response {
	log.WithField("Request", req).Debug("Capabilites")
	return volume.Response{Capabilities: volume.Capability{Scope: "local"}}
}

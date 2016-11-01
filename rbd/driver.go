package rbddriver

import (
	"fmt"
	"os"
	"sync"

	log "github.com/Sirupsen/logrus"
	"github.com/docker/go-plugins-helpers/volume"
)

type RbdDriver struct {
	volume.Driver
	defaultSize string
	pool        string
	mounts      map[string]*rbdImage
	mutex       *sync.Mutex
}

func NewRbdDriver(pool, ds string) (*RbdDriver, error) {
	log.SetLevel(log.DebugLevel)
	log.Debug("Creating new RbdDriver.")

	//startup tasks
	//get mappings
	mappings, err := GetMappings()
	if err != nil {
		log.Errorf(err.Error())
		return nil, fmt.Errorf("Error getting initial mappings.")
	}

	for _, m := range mappings {
		b, err := IsImageLocked(m["pool"] + "/" + m["name"])
		if err != nil {
			log.WithError(err).WithField("mapping", m).Error("Error getting lock status of image.")
		}

		if b {
			/*
				if !lockedbyme {
					emergency unmap
					continue
				}
				get lock share id, rebuild lock
				if not fixed lock, reset refresh loop
				continue
			*/
		}
		/*
			create lock with default refresh (a fixed lock would still be valid and handled above, in theory anyway)

			if !m.usedbycontainer {
				cleanly unmap/unmount
			}
		*/
	}

	return &RbdDriver{pool: pool, defaultSize: ds, mounts: make(map[string]*rbdImage), mutex: &sync.Mutex{}}, nil
}

func (rd *RbdDriver) Create(req volume.Request) volume.Response {
	log.WithField("Request", req).Debug("Create")

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

	_, err := CreateRbdImage(rd.pool, req.Name, size, fs)
	if err != nil {
		log.Errorf(err.Error())
		msg := fmt.Sprintf("Error while creating image %v/%v.", rd.pool, req.Name)
		log.Errorf(msg)
		return volume.Response{Err: msg}
	}

	return volume.Response{Err: ""}
}

func (rd *RbdDriver) List(req volume.Request) volume.Response {
	log.WithField("Requst", req).Debug("List")
	var vols []*volume.Volume

	imgs, err := GetImages(rd.pool)
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

	img, err := LoadRbdImage(rd.pool, req.Name)
	if err != nil {
		log.Errorf(err.Error())
		msg := fmt.Sprintf("Error while loading image %v/%v.", rd.pool, req.Name)
		log.Errorf(msg)
		return volume.Response{Volume: nil, Err: msg}
	}

	vol := &volume.Volume{Name: img.name}

	b, err := img.IsMounted()
	if err == nil && b {
		mp, _ := img.GetMountPoint()
		if mp != "" {
			vol.Mountpoint = mp
		}
	}

	return volume.Response{Volume: vol, Err: ""}
}

func (rd *RbdDriver) Remove(req volume.Request) volume.Response {
	log.WithField("Request", req).Debug("Remove")

	img, err := LoadRbdImage(rd.pool, req.Name)
	if err != nil {
		log.Errorf(err.Error())
		msg := fmt.Sprintf("Error loading image %v.", req.Name)
		log.Errorf(msg)
		return volume.Response{Volume: nil, Err: msg}
	}

	err = img.Remove()
	if err != nil {
		log.Errorf(err.Error())
		msg := fmt.Sprintf("Error occurred while removing the image %v.", img.FullName())
		log.Errorf(msg)
		return volume.Response{Err: msg}
	}

	return volume.Response{Err: ""}
}

func (rd *RbdDriver) Path(req volume.Request) volume.Response {
	log.WithField("Request", req).Debug("Path")

	img, err := LoadRbdImage(rd.pool, req.Name)
	if err != nil {
		log.Errorf(err.Error())
		msg := fmt.Sprintf("Error loading image %v.", req.Name)
		log.Errorf(msg)
		return volume.Response{Volume: nil, Err: msg}
	}

	b, err := img.IsMapped()
	if err != nil {
		log.Errorf(err.Error())
		msg := fmt.Sprintf("Error trying to determine if image %v is mapped.", img.FullName())
		log.Errorf(msg)
		return volume.Response{Err: msg}
	}

	if !b {
		return volume.Response{Err: fmt.Sprintf("Image %v is not mapped to a device.", img.FullName())}
	}

	b, err = img.IsMounted()
	if err != nil {
		log.Errorf(err.Error())
		msg := fmt.Sprintf("Error trying to determine if image %v is mounted.", img.FullName())
		log.Errorf(msg)
		return volume.Response{Err: msg}
	}

	if !b {
		return volume.Response{Err: fmt.Sprintf("Image %v is mapped, but not mounted.", img.FullName())}
	}

	mp, err := img.GetMountPoint()
	if err != nil {
		log.Errorf(err.Error())
		msg := fmt.Sprintf("Error trying to get the mount point of image %v.", img.FullName())
		log.Errorf(msg)
		return volume.Response{Err: msg}
	}

	return volume.Response{Mountpoint: mp, Err: ""}
}

func (rd *RbdDriver) Mount(req volume.MountRequest) volume.Response {
	log.WithField("Request", req).Debug("Mount")
	var err error

	img, err := LoadRbdImage(rd.pool, req.Name)
	if err != nil {
		log.Errorf(err.Error())
		msg := fmt.Sprintf("Error generating data structure for image %v/%v.", rd.pool, req.Name)
		log.Errorf(msg)
		return volume.Response{Err: msg}
	}

	mp, err := img.Mount(req.ID)
	if err != nil {
		log.Errorf(err.Error())
		msg := fmt.Sprintf("Error while mounting image %v.", img.FullName())
		log.Errorf(msg)
		return volume.Response{Err: msg}
	}

	rd.mutex.Lock()
	defer rd.mutex.Unlock()
	rd.mounts[req.Name] = img

	return volume.Response{Mountpoint: mp, Err: ""}
}

func (rd *RbdDriver) Unmount(req volume.UnmountRequest) volume.Response {
	log.WithField("Request", req).Debug("Unmount")

	rd.mutex.Lock()
	defer rd.mutex.Unlock()
	img, ok := rd.mounts[req.Name]
	if !ok {
		msg := fmt.Sprintf("Could not find image object for %v.", req.Name)
		log.Errorf(msg)
		return volume.Response{Err: msg}
	}

	err := img.Unmount(req.ID)
	if err != nil {
		log.Errorf(err.Error())
		msg := fmt.Sprintf("Error unmounting image %v.", img.FullName())
		log.Errorf(msg)
		return volume.Response{Err: msg}
	}

	return volume.Response{Err: ""}
}

func (rd *RbdDriver) Capabilities(req volume.Request) volume.Response {
	log.WithField("Request", req).Debug("Capabilites")
	return volume.Response{Capabilities: volume.Capability{Scope: "global"}}
}

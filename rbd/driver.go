package rbddriver

import (
	"fmt"
	"os"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/docker/go-plugins-helpers/volume"
)

const (
	DRP_DEFAULT_LOCK_REFRESH = 60
	DRP_REFRESH_PERCENT      = 50
)

var (
	DRP_END_OF_TIME = time.Unix(1<<63-62135596801, 999999999)
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

	mnts := make(map[string]*rbdImage)

	//startup tasks
	//get mappings
	mappings, err := GetMappings()
	if err != nil {
		log.Errorf(err.Error())
		return nil, fmt.Errorf("Error getting initial mappings.")
	}

	for _, m := range mappings {
		hn, err := os.Hostname()
		if err != nil {
			log.WithError(err).Error("Error getting my hostname.")
			continue
		}

		image := m["pool"] + "/" + m["name"]
		img, err := LoadRbdImage(image)
		if err != nil {
			log.WithError(err).WithField("image", image).Error("Error loading image.")
			continue
		}

		b, err := img.IsLocked()
		if err != nil {
			log.WithError(err).WithField("image", image).Error("Error getting lock status of image.")
			continue
		}

		if b {
			who, err := img.GetLockHost()
			if err != nil {
				log.WithError(err).WithField("image", image).Error("Error finding image locking host.")
				continue
			}

			if who != hn {
				log.Error("Found a local map that is locked by someone else! Running emergency unmap!")
				err := img.EmergencyUnmap()
				if err != nil {
					log.WithError(err).WithField("image", img.image).Panic("Error while doing an emergency unmap. I hope your data is not corrupted.")
				}
				continue
			}
		}

		tag, err := img.GetCephLockTag()
		if err != nil {
			log.WithError(err).WithField("image", img.image).Error("Error getting lock tag.")
			tag = "dummy_string_tag"
		}

		exp, err := img.GetCephLockExpiration()
		if err != nil {
			log.WithError(err).WithField("image", img.image).Error("Error getting lock expiration.")
			exp = time.Now()
		}

		expireSeconds := DRP_DEFAULT_LOCK_REFRESH
		if exp.Equal(DRP_END_OF_TIME) {
			expireSeconds = 0
		}

		img.activeLock, err = InheritLock(img, tag, expireSeconds)

		mnts[img.image] = img
		/*
			now that we are stable and hold locks on existing maps, we need to see if any are not in use and lose them

			if !m.usedbycontainer {
				cleanly unmap/unmount
			}
		*/
	}

	return &RbdDriver{pool: pool, defaultSize: ds, mounts: mnts, mutex: &sync.Mutex{}}, nil
}

func (rd *RbdDriver) Create(req volume.Request) volume.Response {
	log.WithField("Request", req).Debug("Create")

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
		msg := fmt.Sprintf("Error while creating image %v.", image)
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

	image := rd.pool + "/" + req.Name

	img, err := LoadRbdImage(image)
	if err != nil {
		log.Errorf(err.Error())
		msg := fmt.Sprintf("Error while loading image %v.", image)
		log.Errorf(msg)
		return volume.Response{Volume: nil, Err: msg}
	}

	vol := &volume.Volume{Name: img.ShortName()}

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

	image := rd.pool + "/" + req.Name

	img, err := LoadRbdImage(image)
	if err != nil {
		log.Errorf(err.Error())
		msg := fmt.Sprintf("Error loading image %v.", req.Name)
		log.Errorf(msg)
		return volume.Response{Volume: nil, Err: msg}
	}

	err = img.Remove()
	if err != nil {
		log.Errorf(err.Error())
		msg := fmt.Sprintf("Error occurred while removing the image %v.", image)
		log.Errorf(msg)
		return volume.Response{Err: msg}
	}

	return volume.Response{Err: ""}
}

func (rd *RbdDriver) Path(req volume.Request) volume.Response {
	log.WithField("Request", req).Debug("Path")

	image := rd.pool + "/" + req.Name

	img, err := LoadRbdImage(image)
	if err != nil {
		log.Errorf(err.Error())
		msg := fmt.Sprintf("Error loading image %v.", req.Name)
		log.Errorf(msg)
		return volume.Response{Volume: nil, Err: msg}
	}

	b, err := img.IsMapped()
	if err != nil {
		log.Errorf(err.Error())
		msg := fmt.Sprintf("Error trying to determine if image %v is mapped.", image)
		log.Errorf(msg)
		return volume.Response{Err: msg}
	}

	if !b {
		return volume.Response{Err: fmt.Sprintf("Image %v is not mapped to a device.", image)}
	}

	b, err = img.IsMounted()
	if err != nil {
		log.Errorf(err.Error())
		msg := fmt.Sprintf("Error trying to determine if image %v is mounted.", image)
		log.Errorf(msg)
		return volume.Response{Err: msg}
	}

	if !b {
		return volume.Response{Err: fmt.Sprintf("Image %v is mapped, but not mounted.", image)}
	}

	mp, err := img.GetMountPoint()
	if err != nil {
		log.Errorf(err.Error())
		msg := fmt.Sprintf("Error trying to get the mount point of image %v.", image)
		log.Errorf(msg)
		return volume.Response{Err: msg}
	}

	return volume.Response{Mountpoint: mp, Err: ""}
}

func (rd *RbdDriver) Mount(req volume.MountRequest) volume.Response {
	log.WithField("Request", req).Debug("Mount")
	var err error

	image := rd.pool + "/" + req.Name

	img, err := LoadRbdImage(image)
	if err != nil {
		log.Errorf(err.Error())
		msg := fmt.Sprintf("Error generating data structure for image %v.", image)
		log.Errorf(msg)
		return volume.Response{Err: msg}
	}

	mp, err := img.Mount(req.ID)
	if err != nil {
		log.Errorf(err.Error())
		msg := fmt.Sprintf("Error while mounting image %v.", image)
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

	image := rd.pool + "/" + req.Name

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
		msg := fmt.Sprintf("Error unmounting image %v.", image)
		log.Errorf(msg)
		return volume.Response{Err: msg}
	}

	return volume.Response{Err: ""}
}

func (rd *RbdDriver) Capabilities(req volume.Request) volume.Response {
	log.WithField("Request", req).Debug("Capabilites")
	return volume.Response{Capabilities: volume.Capability{Scope: "global"}}
}

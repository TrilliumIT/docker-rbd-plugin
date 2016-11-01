package rbddriver

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"

	log "github.com/Sirupsen/logrus"
)

const (
	DRP_DEFAULT_LOCK_REFRESH = 60
)

type rbdImage struct {
	pool string
	name string
	lock *rbdLock
}

func LoadRbdImage(pool, name string) (*rbdImage, error) {
	fullname := pool + "/" + name

	b, err := ImageExists(pool, name)
	if err != nil {
		log.Errorf(err.Error())
		return nil, fmt.Errorf("Error trying to determine if image %v already exists.", fullname)
	}

	if !b {
		return nil, fmt.Errorf("Image %v does not exists, cannot load.", fullname)
	}

	return &rbdImage{pool: pool, name: name}, nil
}

func CreateRbdImage(pool, name, size, fs string) (*rbdImage, error) {
	fullname := pool + "/" + name

	b, err := ImageExists(pool, name)
	if err != nil {
		log.Errorf(err.Error())
		return nil, fmt.Errorf("Error trying to determine if image %v already exists.", fullname)
	}

	if b {
		log.Warningf("Tried to create existing image %v, loading it instead.", fullname)
		return LoadRbdImage(pool, name)
	}

	err = exec.Command("rbd", "create", fullname, "--size", size).Run()
	if err != nil {
		log.Errorf(err.Error())
		return nil, fmt.Errorf("Error trying to create the image %v.", fullname)
	}
	defer func() {
		if err != nil {
			log.Errorf("Detected error after creating image %v, removing.")
			_ = exec.Command("rbd", "remove", fullname).Run()
		}
	}()

	img, err := LoadRbdImage(pool, name)
	if err != nil {
		log.Errorf(err.Error())
		return nil, fmt.Errorf("Failed to load the newly created blank image %v.", fullname)
	}

	dev, err := img.mapDevice("create_mkfs_lock")
	if err != nil {
		log.Errorf(err.Error())
		return nil, fmt.Errorf("Failed to map the newly created blank image %v.", fullname)
	}
	defer func() {
		if err != nil {
			log.Errorf("Detected error after mapping image %v, unmapping.", fullname)
			_ = img.unmapDevice("create_mkfs_lock")
		}
	}()

	err = exec.Command("mkfs."+fs, dev).Run()
	if err != nil {
		log.Errorf(err.Error())
		return nil, fmt.Errorf("Failed to create the filesystem on device %v for image %v.", dev, fullname)
	}

	err = img.unmapDevice("create_mkfs_lock")
	if err != nil {
		log.Errorf(err.Error())
		//set err back to nil to prevent our image from being destroyed
		//it should be a good image at this point
		err = nil
		return img, fmt.Errorf("Failed to unmap device %v after creating image %v.", dev, fullname)
	}

	return img, nil
}

func (img *rbdImage) IsMapped() (bool, error) {
	mapping, err := img.GetMapping()
	if err != nil {
		log.Errorf("Failed to get mapping for image %v.", img.FullName())
		return false, err
	}

	if mapping == nil {
		return false, nil
	}

	return true, nil
}

func (img *rbdImage) GetDevice() (string, error) {
	mapping, err := img.GetMapping()
	if err != nil {
		log.Errorf("Failed to get mapping for image %v.", img.FullName())
		return "", err
	}

	if mapping == nil {
		return "", fmt.Errorf("Image %v is not mapped, cannot retrieve the device.", img.FullName())
	}

	return mapping["device"], nil
}

func (img *rbdImage) IsMounted() (bool, error) {
	b, err := img.IsMapped()
	if err != nil {
		log.Errorf(err.Error())
		return false, fmt.Errorf("Error trying to determine if image %v is mapped.", img.FullName())
	}

	if !b {
		return false, nil
	}

	dev, err := img.GetDevice()
	if err != nil {
		log.Errorf(err.Error())
		return false, fmt.Errorf("Failed to get device from image %v.", img.FullName())
	}

	mounts, err := GetMounts()
	if err != nil {
		log.Errorf(err.Error())
		return false, fmt.Errorf("Failed to get the system mounts.")
	}

	_, ok := mounts[dev]

	return ok, nil
}

func (img *rbdImage) GetMountPoint() (string, error) {
	dev, err := img.GetDevice()
	if err != nil {
		return "", fmt.Errorf("Failed to get device from image %v.", img.FullName())
	}

	mounts, err := GetMounts()
	if err != nil {
		log.Errorf(err.Error())
		return "", fmt.Errorf("Failed to get the system mounts.")
	}

	mnt, ok := mounts[dev]
	if !ok {
		return "", fmt.Errorf("Device %v is not mounted.", dev)
	}

	return mnt.MountPoint, nil
}

func (img *rbdImage) FullName() string {
	return img.pool + "/" + img.name
}

func (img *rbdImage) mapDevice(id string) (string, error) {
	//TODO: check for locks

	b, err := img.IsMapped()
	if err != nil {
		log.Errorf(err.Error())
		return "", fmt.Errorf("Failed to detrimine if image %v is already mapped.", img.FullName())
	}

	if b {
		dev, err := img.GetDevice()
		if err != nil {
			log.Errorf(err.Error())
			return "", fmt.Errorf("Image %v is already mapped and failed to get the device.", img.FullName())
		}

		//TODO: Acquire a lock in this case?

		log.Warningf("Image %v is already mapped to %v.", img.FullName(), dev)

		return dev, nil
	}

	refresh := DRP_DEFAULT_LOCK_REFRESH
	srefresh := os.Getenv("DRP_LOCK_REFRESH")
	if srefresh != "" {
		refresh, err = strconv.Atoi(srefresh)
		if err != nil {
			log.Warningf("Error while parsing DRP_LOCK_REFRESH with value %v to int.", srefresh)
		}
	}

	err = img.lockImage(id, refresh)
	if err != nil {
		log.Errorf(err.Error())
		return "", fmt.Errorf("Error acquiring lock on image %v with id %v.", img.FullName(), id)
	}
	defer func() {
		if err != nil {
			_ = img.unlockImage()
		}
	}()

	out, err := exec.Command("rbd", "map", img.FullName()).Output()
	if err != nil {
		log.Errorf(err.Error())
		return "", fmt.Errorf("Failed to map the image %v.", img.FullName())
	}

	return strings.TrimSpace(string(out)), nil
}

func (img *rbdImage) unmapDevice(lockid string) error {
	b, err := img.IsMounted()
	if err != nil {
		log.Errorf(err.Error())
		return fmt.Errorf("Failed to determine if image %v is currently mounted", img.FullName())
	}

	if b {
		return fmt.Errorf("Cannot unmap image %v because it is currently mounted.", img.FullName())
	}

	b, err = img.IsMapped()
	if err != nil {
		log.Errorf(err.Error())
		return fmt.Errorf("Failed to determine if image %v is mapped.", img.FullName())
	}

	if !b {
		err = img.unlockImage()
		if err != nil {
			log.Errorf(err.Error())
			return fmt.Errorf("Error unlocking non mapped image %v.", img.FullName())
		}

		return fmt.Errorf("Image %v is not currently mapped to a device, removed lock if present.", img.FullName())
	}

	err = exec.Command("rbd", "unmap", img.FullName()).Run()
	if err != nil {
		log.Errorf(err.Error())

		return fmt.Errorf("Error while trying to unmap the image %v.", img.FullName())
	}

	err = img.unlockImage()
	if err != nil {
		log.Errorf(err.Error())
		return fmt.Errorf("Error unlocking image %v.", img.FullName())
	}

	return nil
}

func (img *rbdImage) Remove() error {
	b, err := img.IsMounted()
	if err != nil {
		log.Errorf(err.Error())
		return fmt.Errorf("Error trying to determine if image %v is currently mounted", img.FullName())
	}

	if b {
		return fmt.Errorf("Cannot remove image %v because it is currently mounted.", img.FullName())
	}

	b, err = img.IsMapped()
	if err != nil {
		log.Errorf(err.Error())
		return fmt.Errorf("Error trying to determine if image %v is mapped.", img.FullName())
	}

	if b {
		return fmt.Errorf("Cannot remove image %v because it is currently mapped.", img.FullName())
	}

	if img.lock != nil {
		return fmt.Errorf("Cannot remove image %v because it is currently locked by %v.", img.FullName(), img.lock.hostname)
	}

	err = exec.Command("rbd", "remove", img.FullName()).Run()
	if err != nil {
		log.Errorf("Error while trying to remove image %v.", img.FullName())
		return err
	}

	return nil
}

func (img *rbdImage) GetMapping() (map[string]string, error) {
	mappings, err := GetMappings()
	if err != nil {
		log.Errorf("Failed to retrive the rbd mappings.")
		return nil, err
	}

	for _, rbd := range mappings {
		if rbd["pool"] != img.pool {
			continue
		}

		if rbd["name"] == img.name {
			return rbd, nil
		}
	}

	return nil, nil
}

func (img *rbdImage) lockImage(tag string, refresh int) error {
	//TODO: use shared lock mechanism for refreshing, expiring, and reaping locks
	lock, err := AcquireLock(img.FullName(), tag, refresh)
	if err != nil {
		log.Errorf(err.Error())
		return fmt.Errorf("Error while acquiring a lock for image %v with tag %v and refresh %v.", img.FullName(), tag, refresh)
	}

	img.lock = lock

	return nil
}

func (img *rbdImage) unlockImage() error {
	err := img.lock.release()
	if err != nil {
		log.Errorf(err.Error())
		return fmt.Errorf("Error while releasing a lock for image %v.", img.FullName())
	}

	img.lock = nil

	return nil
}

func (img *rbdImage) Mount(lockid string) (string, error) {
	b, err := img.IsMounted()
	if err != nil {
		log.Errorf(err.Error())
		return "", fmt.Errorf("Error determining if image %v is already mounted.", img.FullName())
	}

	if b {
		mp, err := img.GetMountPoint()
		if err != nil {
			log.Errorf(err.Error())
			return "", fmt.Errorf("Device for image %v is already mounted, failed to get the mountpoint.")
		}
		log.Warningf("Image %v is already mounted at %v.", img.FullName(), mp)
		return mp, nil
	}

	mp := os.Getenv("DRB_VOLUME_DIR")
	if mp == "" {
		mp = "/var/lib/docker-volumes/rbd"
	}
	mp += "/" + img.FullName()

	err = os.MkdirAll(mp, 0755)
	if err != nil {
		log.Errorf(err.Error())
		return "", fmt.Errorf("Error creating new mount point directory at %v.", mp)
	}

	dev, err := img.mapDevice(lockid)
	if err != nil {
		log.Errorf(err.Error())
		return "", fmt.Errorf("Error mapping image %v to device.", img.FullName())
	}
	defer func() {
		if err != nil {
			_ = img.unmapDevice(lockid)
		}
	}()

	//TODO: use blkid to detect fs type
	fs := os.Getenv("DRP_DEFAULT_FS")
	if fs == "" {
		fs = "xfs"
	}

	err = syscall.Mount(dev, mp, fs, 0, "")
	if err != nil {
		log.Errorf(err.Error())
		return "", fmt.Errorf("Error while trying to mount device %v.", dev)
	}

	return "", nil
}

func (img *rbdImage) Unmount(lockid string) error {
	b, err := img.IsMounted()
	if err != nil {
		log.Errorf(err.Error())
		return fmt.Errorf("Error determining if image %v is mounted.", img.FullName())
	}

	if !b {
		err = img.unmapDevice(lockid)
		if err != nil {
			log.Errorf(err.Error())
			return fmt.Errorf("Error unmapping image %v from device.", img.FullName())
		}
		log.Warningf("Tried to unmount a device that was not mounted for image %v.", img.FullName())
		return nil
	}

	mp, err := img.GetMountPoint()
	if err != nil {
		log.Errorf(err.Error())
		return fmt.Errorf("Error determining the path of device for image %v.", img.FullName())
	}

	err = syscall.Unmount(mp, 0)
	if err != nil {
		log.Errorf(err.Error())
		return fmt.Errorf("Error while trying to unmount image %v from path %v.", img.FullName, mp)
	}

	err = img.unmapDevice(lockid)
	if err != nil {
		log.Errorf(err.Error())
		return fmt.Errorf("Error unmapping image %v.", img.FullName())
	}

	return nil
}

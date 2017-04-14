package rbddriver

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"

	log "github.com/Sirupsen/logrus"
)

type rbdImage struct {
	image      string
	activeLock *rbdLock
	users      map[string]struct{}
}

func LoadRbdImage(image string) (*rbdImage, error) {
	b, err := ImageExists(image)
	if err != nil {
		log.Errorf(err.Error())
		return nil, fmt.Errorf("Error trying to determine if image %v already exists.", image)
	}

	if !b {
		return nil, fmt.Errorf("Image %v does not exists, cannot load.", image)
	}

	return &rbdImage{image: image, users: make(map[string]struct{})}, nil
}

func CreateRbdImage(image, size, fs string) (*rbdImage, error) {
	b, err := ImageExists(image)
	if err != nil {
		log.Errorf(err.Error())
		return nil, fmt.Errorf("Error trying to determine if image %v already exists.", image)
	}

	if b {
		log.Warningf("Tried to create existing image %v, loading it instead.", image)
		return LoadRbdImage(image)
	}

	log.Debugf("Executing: rbd create %v --size %v", image, size)
	err = exec.Command("rbd", "create", image, "--size", size).Run()
	if err != nil {
		log.Errorf(err.Error())
		return nil, fmt.Errorf("Error trying to create the image %v.", image)
	}
	defer func() {
		if err != nil {
			log.Errorf("Detected error after creating image %v, removing.")
			_ = exec.Command("rbd", "remove", image).Run()
		}
	}()

	img, err := LoadRbdImage(image)
	if err != nil {
		log.Errorf(err.Error())
		return nil, fmt.Errorf("Failed to load the newly created blank image %v.", image)
	}

	dev, err := img.mapDevice()
	if err != nil {
		log.Errorf(err.Error())
		return nil, fmt.Errorf("Failed to map the newly created blank image %v.", image)
	}
	defer func() {
		if err != nil {
			log.Errorf("Detected error after mapping image %v, unmapping.", image)
			_ = img.unmapDevice()
		}
	}()

	err = exec.Command("mkfs."+fs, dev).Run()
	if err != nil {
		log.Errorf(err.Error())
		return nil, fmt.Errorf("Failed to create the filesystem on device %v for image %v.", dev, image)
	}

	err = img.unmapDevice()
	if err != nil {
		log.Errorf(err.Error())
		//set err back to nil to prevent our image from being destroyed
		//it should be a good image at this point
		err = nil
		return img, fmt.Errorf("Failed to unmap device %v after creating image %v.", dev, image)
	}

	return img, nil
}

func (img *rbdImage) ShortName() string {
	names := strings.Split(img.image, "/")
	if len(names) > 0 {
		return strings.Join(names[1:], "/")
	}
	return names[0]
}

func (img *rbdImage) PoolName() string {
	names := strings.Split(img.image, "/")
	return names[0]
}

func (img *rbdImage) IsMapped() (bool, error) {
	mappings, err := GetMappings(img.PoolName())
	if err != nil {
		log.Errorf(err.Error())
		return false, fmt.Errorf("Error getting rbd mappings.")
	}

	for _, v := range mappings {
		if img.image == v["pool"]+"/"+v["name"] {
			return true, nil
		}
	}

	return false, nil
}

func (img *rbdImage) GetDevice() (string, error) {
	mapping, err := img.GetMapping()
	if err != nil {
		log.Errorf("Failed to get mapping for image %v.", img.image)
		return "", err
	}

	if mapping == nil {
		return "", fmt.Errorf("Image %v is not mapped, cannot retrieve the device.", img.image)
	}

	return mapping["device"], nil
}

func (img *rbdImage) IsMounted() (bool, error) {
	b, err := img.IsMapped()
	if err != nil {
		log.Errorf(err.Error())
		return false, fmt.Errorf("Error trying to determine if image %v is mapped.", img.image)
	}

	if !b {
		return false, nil
	}

	dev, err := img.GetDevice()
	if err != nil {
		log.Errorf(err.Error())
		return false, fmt.Errorf("Failed to get device from image %v.", img.image)
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
		return "", fmt.Errorf("Failed to get device from image %v.", img.image)
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

func (img *rbdImage) mapDevice() (string, error) {
	b, err := img.IsLocked()
	if err != nil {
		log.Errorf(err.Error())
		return "", fmt.Errorf("Error trying to determine if image %v was already locked.", img.image)
	}

	if b {
		hn, err := os.Hostname()
		if err != nil {
			log.WithError(err).Error("Error getting my hostname.")
		}

		who, err := img.GetLockHost()
		if err != nil {
			log.WithError(err).WithField("image", img.image).Error("Error finding image locking host.")
		}

		if who != hn {
			return "", fmt.Errorf("Cannot map a locked image.")
		}
		log.Warningf("Discovered a lock on image %v, but it's me. Continuing anyway", img.image)
	}

	refresh := DRP_DEFAULT_LOCK_REFRESH
	srefresh := os.Getenv("DRP_LOCK_REFRESH")
	if srefresh != "" {
		refresh, err = strconv.Atoi(srefresh)
		if err != nil {
			log.Warningf("Error while parsing DRP_LOCK_REFRESH with value %v to int, using default.", srefresh)
		}
	}

	b, err = img.IsMapped()
	if err != nil {
		log.Errorf(err.Error())
		return "", fmt.Errorf("Failed to detrimine if image %v is already mapped.", img.image)
	}

	if b {
		dev, err := img.GetDevice()
		if err != nil {
			log.Errorf(err.Error())
			return "", fmt.Errorf("Image %v is already mapped and failed to get the device.", img.image)
		}

		log.Warningf("Image %v is already mapped to %v. Acquiring a lock for it.", img.image, dev)

		err = img.lock(refresh)
		if err != nil {
			log.Errorf(err.Error())
			return "", fmt.Errorf("Error acquiring lock on image %v.", img.image)
		}

		return dev, nil
	}

	err = img.lock(refresh)
	if err != nil {
		log.Errorf(err.Error())
		return "", fmt.Errorf("Error acquiring lock on image %v.", img.image)
	}
	defer func() {
		if err != nil {
			_ = img.unlock()
		}
	}()

	out, err := exec.Command("rbd", "map", img.image).Output()
	if err != nil {
		log.Errorf(err.Error())
		return "", fmt.Errorf("Failed to map the image %v.", img.image)
	}

	return strings.TrimSpace(string(out)), nil
}

func (img *rbdImage) unmapDevice() error {
	b, err := img.IsMounted()
	if err != nil {
		log.Errorf(err.Error())
		return fmt.Errorf("Failed to determine if image %v is currently mounted", img.image)
	}

	if b {
		return fmt.Errorf("Cannot unmap image %v because it is currently mounted.", img.image)
	}

	b, err = img.IsMapped()
	if err != nil {
		log.Errorf(err.Error())
		return fmt.Errorf("Failed to determine if image %v is mapped.", img.image)
	}

	if !b {
		err = img.unlock()
		if err != nil {
			log.Errorf(err.Error())
			return fmt.Errorf("Error unlocking non mapped image %v.", img.image)
		}

		return fmt.Errorf("Image %v is not currently mapped to a device, removed lock if present.", img.image)
	}

	err = exec.Command("rbd", "unmap", img.image).Run()
	if err != nil {
		log.Errorf(err.Error())

		return fmt.Errorf("Error while trying to unmap the image %v.", img.image)
	}

	err = img.unlock()
	if err != nil {
		log.Errorf(err.Error())
		return fmt.Errorf("Error unlocking image %v.", img.image)
	}

	return nil
}

func (img *rbdImage) Remove() error {
	b, err := img.IsMounted()
	if err != nil {
		log.Errorf(err.Error())
		return fmt.Errorf("Error trying to determine if image %v is currently mounted", img.image)
	}

	if b {
		return fmt.Errorf("Cannot remove image %v because it is currently mounted.", img.image)
	}

	b, err = img.IsMapped()
	if err != nil {
		log.Errorf(err.Error())
		return fmt.Errorf("Error trying to determine if image %v is mapped.", img.image)
	}

	if b {
		return fmt.Errorf("Cannot remove image %v because it is currently mapped.", img.image)
	}

	b, err = img.IsLocked()
	if err != nil {
		log.Errorf(err.Error())
		return fmt.Errorf("Error tyring to determine if image %v is locked.", img.image)
	}

	if b {
		who := ""
		who, err = img.GetLockHost()
		if err != nil {
			log.WithError(err).WithField("image", img.image).Error("Error finding image locking host.")
		}
		return fmt.Errorf("Cannot remove image %v because it is currently locked by %v.", img.image, who)
	}

	err = exec.Command("rbd", "remove", img.image).Run()
	if err != nil {
		log.Errorf("Error while trying to remove image %v.", img.image)
		return err
	}

	return nil
}

func (img *rbdImage) GetMapping() (map[string]string, error) {
	mappings, err := GetMappings(img.PoolName())
	if err != nil {
		log.Errorf(err.Error())
		return nil, fmt.Errorf("Error getting rbd mappings.")
	}

	for _, v := range mappings {
		if img.image == v["pool"]+"/"+v["name"] {
			return v, nil
		}
	}

	return nil, fmt.Errorf("Image %v doesn't seem to be mapped.", img.image)
}

func (img *rbdImage) lock(refresh int) error {
	lock, err := AcquireLock(img, refresh)
	if err != nil {
		log.Errorf(err.Error())
		return fmt.Errorf("Error while acquiring a lock for image %v with refresh %v.", img.image, refresh)
	}

	img.activeLock = lock

	return nil
}

func (img *rbdImage) unlock() error {
	err := img.activeLock.release()
	if err != nil {
		log.Errorf(err.Error())
		return fmt.Errorf("Error while releasing a lock for image %v.", img.image)
	}

	img.activeLock = nil

	return nil
}

func (img *rbdImage) Mount(mountid string) (string, error) {
	b, err := img.IsMounted()
	if err != nil {
		log.Errorf(err.Error())
		return "", fmt.Errorf("Error determining if image %v is already mounted.", img.image)
	}

	if b {
		mp, err := img.GetMountPoint()
		if err != nil {
			log.Errorf(err.Error())
			return "", fmt.Errorf("Device for image %v is already mounted, failed to get the mountpoint.")
		}
		img.users[mountid] = struct{}{}
		return mp, nil
	}

	mp := os.Getenv("DRB_VOLUME_DIR")
	if mp == "" {
		mp = "/var/lib/docker-volumes/rbd"
	}
	mp += "/" + img.image

	err = os.MkdirAll(mp, 0755)
	if err != nil {
		log.Errorf(err.Error())
		return mp, fmt.Errorf("Error creating new mount point directory at %v.", mp)
	}

	dev, err := img.mapDevice()
	if err != nil {
		log.Errorf(err.Error())
		return mp, fmt.Errorf("Error mapping image %v to device.", img.image)
	}
	defer func() {
		if err != nil {
			_ = img.unmapDevice()
		}
	}()

	out, err := exec.Command("blkid", "-s", "TYPE", "-o", "value", dev).Output()
	if err != nil {
		log.Errorf(err.Error())
		return dev, fmt.Errorf("Error running blkid to determine the filesystem type of image %v on dev %v.", img.image, dev)
	}

	fs := strings.TrimSpace(string(out))

	log.Infof("Mounting device %v to %v as %v filesystem.", dev, mp, fs)
	err = syscall.Mount(dev, mp, fs, 0, "")
	if err != nil {
		log.Errorf(err.Error())
		return mp, fmt.Errorf("Error while trying to mount device %v.", dev)
	}

	img.users[mountid] = struct{}{}
	return mp, nil
}

func (img *rbdImage) Unmount(mountid string) error {
	if mountid != "" {
		delete(img.users, mountid)
	}

	if mountid == "" {
		img.users = make(map[string]struct{})
	}

	if len(img.users) > 0 {
		return nil
	}

	b, err := img.IsMounted()
	if err != nil {
		log.Errorf(err.Error())
		return fmt.Errorf("Error determining if image %v is mounted.", img.image)
	}

	if b {
		mp, err := img.GetMountPoint()
		if err != nil {
			log.Errorf(err.Error())
			return fmt.Errorf("Error determining the path of device for image %v.", img.image)
		}

		err = syscall.Unmount(mp, 0)
		if err != nil {
			log.Errorf(err.Error())
			return fmt.Errorf("Error while trying to unmount image %v from path %v.", img.image, mp)
		}
	}

	b, err = img.IsMapped()
	if err != nil {
		log.Errorf(err.Error())
		return fmt.Errorf("Error determining if image %v is mapped.", img.image)
	}

	if b {
		err = img.unmapDevice()
		if err != nil {
			log.Errorf(err.Error())
			return fmt.Errorf("Error unmapping image %v.", img.image)
		}
	}

	err = img.unlock()
	if err != nil {
		log.Errorf(err.Error())
		return fmt.Errorf("Error unlocking image %v.", img.image)
	}

	return nil
}

func (img *rbdImage) GetLockHost() (string, error) {
	locks, err := img.GetValidLocks()
	if err != nil {
		log.Errorf(err.Error())
		return "", fmt.Errorf("Error while getting valid locks for image %v.", img.image)
	}

	for k := range locks {
		lock := strings.Split(k, ",")
		return lock[0], nil
	}

	return "", fmt.Errorf("Image %v is not currently locked.", img.image)
}

func (img *rbdImage) GetAllLocks() (map[string]map[string]string, error) {
	bytes, err := exec.Command("rbd", "lock", "list", "--format", "json", img.image).Output()
	if err != nil {
		log.Errorf(err.Error())
		return nil, fmt.Errorf("Failed to get locks for image %v.", img.image)
	}

	var locks map[string]map[string]string
	err = json.Unmarshal(bytes, &locks)
	if err != nil {
		log.Errorf(err.Error())
		return nil, fmt.Errorf("Failed to unmarshal json: %v", string(bytes))
	}

	return locks, nil
}

func (img *rbdImage) GetValidLocks() (map[string]map[string]string, error) {
	locks, err := img.GetAllLocks()
	if err != nil {
		log.Errorf(err.Error())
		return nil, fmt.Errorf("Error while getting locks for image %v.", img.image)
	}

	vlocks := make(map[string]map[string]string)

	for k, v := range locks {
		lock := strings.Split(k, ",")

		t, err := time.Parse(time.RFC3339Nano, lock[1])
		if err != nil {
			log.Warningf(err.Error())
			log.Warningf("Error while parsing time from lock id %v.", k)
			continue
		}

		if time.Now().Before(t) {
			vlocks[k] = v
		}
	}

	return vlocks, nil
}

func (img *rbdImage) IsLocked() (bool, error) {
	locks, err := img.GetValidLocks()
	if err != nil {
		log.Errorf(err.Error())
		return false, fmt.Errorf("Error while getting valid locks for image %v.", img.image)
	}

	return len(locks) > 0, nil
}

func (img *rbdImage) EmergencyUnmap(containerid string) error {
	dev, err := img.GetDevice()
	if err != nil {
		log.Errorf(err.Error())
		return fmt.Errorf("Error getting image %v mapping.", img.image)
	}

	if containerid != "" {
		log.Info("Killing container with id %v.", containerid)
		err = exec.Command("docker", "kill", containerid).Run()
		if err != nil {
			log.WithError(err).Warningf("Error killing container %v.", containerid)
		}
	}

	log.Infof("Killing all processes accessing %v.", dev)
	err = exec.Command("sh", "-c", fmt.Sprintf("kill -9 $(lsof -t %v)", dev)).Run()
	if err != nil {
		log.WithError(err).Error("Error killing all processes accessing the device.")
	}

	mp, err := img.GetMountPoint()
	if err != nil {
		log.WithError(err).WithField("image", img.image).Error("Error getting the mountpoint.")
	}

	if mp != "" {
		log.WithField("image", img.image).Info("Attempting emergency unmount.")
		err = syscall.Unmount(mp, 0)
		if err != nil {
			log.WithError(err).WithField("image", img.image).Error("Error unmounting.")
		}
	}

	log.Infof("Attempting unmap of image %v.", img.image)
	err = exec.Command("rbd", "unmap", img.image).Run()
	if err != nil {
		log.Errorf(err.Error())
		return fmt.Errorf("Error while trying to unmap the image %v.", img.image)
	}

	return nil
}

func (img *rbdImage) reapLocks() error {
	locks, err := img.GetAllLocks()
	if err != nil {
		log.Errorf(err.Error())
		return fmt.Errorf("Error while getting locks for image %v.", img.image)
	}

	for k, v := range locks {
		lock := strings.Split(k, ",")

		t, err := time.Parse(time.RFC3339Nano, lock[1])
		if err != nil {
			log.WithError(err).WithField("lock id", k).Warning("Error while parsing time from lock id.")
			continue
		}

		if time.Now().After(t) {
			log.WithFields(log.Fields{
				"lock id": k,
				"locker":  v["locker"],
				"address": v["address"],
			}).Info("Reaping expired lock.")
			img.removeLock(k, v["locker"])
		}
	}

	return nil
}

func (img *rbdImage) removeLock(id, locker string) error {
	err := exec.Command("rbd", "lock", "rm", img.image, id, locker).Run()
	if err != nil {
		log.Errorf(err.Error())
		return fmt.Errorf("Error removing lock from image %v with id %v.", img.image, id)
	}

	return nil
}

func (img *rbdImage) GetCephLockExpiration() (time.Time, error) {
	exp := time.Now()

	locks, err := img.GetValidLocks()
	if err != nil {
		log.Error(err.Error())
		return exp, fmt.Errorf("Error getting valid locks for image %v.", img.image)
	}

	err = fmt.Errorf("No valid locks found for image %v.", img.image)
	for k := range locks {
		lock := strings.Split(k, ",")
		var t time.Time
		t, err = time.Parse(time.RFC3339Nano, lock[1])
		if t.After(exp) {
			exp = t
		}
	}

	return exp, err
}

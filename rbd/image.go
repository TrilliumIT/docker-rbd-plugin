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

//RbdImage represents a ceph rbd
type RbdImage struct {
	image      string
	activeLock *RbdLock
	users      *rbdUsers
}

//LoadRbdImage loads an existing rbd image from ceph and returns it
func LoadRbdImage(image string) (*RbdImage, error) {
	b, err := ImageExists(image)
	if err != nil {
		log.Errorf(err.Error())
		return nil, fmt.Errorf("error trying to determine if image %v already exists", image)
	}

	if !b {
		return nil, fmt.Errorf("image %v does not exists, cannot load", image)
	}

	return &RbdImage{image: image, users: &rbdUsers{users: make(map[string]struct{})}}, nil
}

//CreateRbdImage creates a new rbd image in ceph
func CreateRbdImage(image, size, fs string) (*RbdImage, error) {
	b, err := ImageExists(image)
	if err != nil {
		log.Errorf(err.Error())
		return nil, fmt.Errorf("error trying to determine if image %v already exists", image)
	}

	if b {
		log.Warningf("tried to create existing image %v, loading it instead", image)
		return LoadRbdImage(image)
	}

	log.Debugf("executing: rbd create %v --size %v", image, size)
	err = exec.Command(DrpRbdBinPath, "create", image, "--size", size).Run() //nolint: gas
	if err != nil {
		log.Errorf(err.Error())
		return nil, fmt.Errorf("error trying to create the image %v", image)
	}
	defer func() {
		if err != nil {
			log.Errorf("detected error after creating image %v, removing")
			_ = exec.Command(DrpRbdBinPath, "remove", image).Run() //nolint: gas
		}
	}()

	img, err := LoadRbdImage(image)
	if err != nil {
		log.Errorf(err.Error())
		return nil, fmt.Errorf("failed to load the newly created blank image %v", image)
	}

	dev, err := img.mapDevice()
	if err != nil {
		log.Errorf(err.Error())
		return nil, fmt.Errorf("failed to map the newly created blank image %v", image)
	}
	defer func() {
		if err != nil {
			log.Errorf("detected error after mapping image %v, unmapping", image)
			_ = img.unmapDevice()
		}
	}()

	err = exec.Command("mkfs."+fs, dev).Run() //nolint: gas
	if err != nil {
		log.Errorf(err.Error())
		return nil, fmt.Errorf("failed to create the filesystem on device %v for image %v", dev, image)
	}

	err = img.unmapDevice()
	if err != nil {
		log.Errorf(err.Error())
		//set err back to nil to prevent our image from being destroyed
		//it should be a good image at this point
		err = nil
		return img, fmt.Errorf("failed to unmap device %v after creating image %v", dev, image)
	}

	return img, nil
}

//ShortName returns the short name of an rbd image (the <name> from the form <pool>/<name>)
func (img *RbdImage) ShortName() string {
	names := strings.Split(img.image, "/")
	if len(names) > 0 {
		return strings.Join(names[1:], "/")
	}
	return names[0]
}

//PoolName returns the pool name of an rbd image (the <pool> from the form <pool>/<name>)
func (img *RbdImage) PoolName() string {
	names := strings.Split(img.image, "/")
	return names[0]
}

//IsMapped returns true if the image is mapped into the kernel
func (img *RbdImage) IsMapped() (bool, error) {
	mappings, err := GetMappings(img.PoolName())
	if err != nil {
		log.Errorf(err.Error())
		return false, fmt.Errorf("error getting rbd mappings")
	}

	for _, v := range mappings {
		if img.image == v["pool"]+"/"+v["name"] {
			return true, nil
		}
	}

	return false, nil
}

//GetDevice returns the device name of a mapped device (/dev/rbdX)
//TODO: explore the possibility of using the symlinks at (/dev/rbd/<pool>/<name>)
//TODO: that might be easier
func (img *RbdImage) GetDevice() (string, error) {
	mapping, err := img.GetMapping()
	if err != nil {
		log.Errorf("failed to get mapping for image %v", img.image)
		return "", err
	}

	if mapping == nil {
		return "", fmt.Errorf("image %v is not mapped, cannot retrieve the device", img.image)
	}

	return mapping["device"], nil
}

//IsMounted returns true if the device is mounted in the kernel
func (img *RbdImage) IsMounted() (bool, error) {
	b, err := img.IsMapped()
	if err != nil {
		log.Errorf(err.Error())
		return false, fmt.Errorf("error trying to determine if image %v is mapped", img.image)
	}

	if !b {
		return false, nil
	}

	dev, err := img.GetDevice()
	if err != nil {
		log.Errorf(err.Error())
		return false, fmt.Errorf("failed to get device from image %v", img.image)
	}

	mounts, err := GetMounts()
	if err != nil {
		log.Errorf(err.Error())
		return false, fmt.Errorf("failed to get the system mounts")
	}

	_, ok := mounts[dev]

	return ok, nil
}

//GetMountPoint returns the path to which the device is mounted
func (img *RbdImage) GetMountPoint() (string, error) {
	dev, err := img.GetDevice()
	if err != nil {
		return "", fmt.Errorf("failed to get device from image %v", img.image)
	}

	mounts, err := GetMounts()
	if err != nil {
		log.Errorf(err.Error())
		return "", fmt.Errorf("failed to get the system mounts")
	}

	mnt, ok := mounts[dev]
	if !ok {
		return "", fmt.Errorf("device %v is not mounted", dev)
	}

	return mnt.MountPoint, nil
}

func (img *RbdImage) mapDevice() (string, error) {
	b, err := img.IsLocked()
	if err != nil {
		log.Errorf(err.Error())
		return "", fmt.Errorf("error trying to determine if image %v was already locked", img.image)
	}

	if b {
		return "", fmt.Errorf("cannot map a locked image")
	}

	refresh := DrpDefaultLockRefresh
	srefresh := os.Getenv("DRP_LOCK_REFRESH")
	if srefresh != "" {
		refresh, err = strconv.Atoi(srefresh)
		if err != nil {
			log.Warningf("error while parsing DRP_LOCK_REFRESH with value %v to int, using default", srefresh)
		}
	}

	b, err = img.IsMapped()
	if err != nil {
		log.Errorf(err.Error())
		return "", fmt.Errorf("failed to detrimine if image %v is already mapped", img.image)
	}

	if b {
		var dev string
		dev, err = img.GetDevice()
		if err != nil {
			log.Errorf(err.Error())
			return "", fmt.Errorf("image %v is already mapped and failed to get the device", img.image)
		}

		log.Warningf("image %v is already mapped to %v, acquiring a lock for it", img.image, dev)

		err = img.lock(refresh)
		if err != nil {
			log.Errorf(err.Error())
			return "", fmt.Errorf("error acquiring lock on image %v", img.image)
		}

		return dev, nil
	}

	err = img.lock(refresh)
	if err != nil {
		log.Errorf(err.Error())
		return "", fmt.Errorf("error acquiring lock on image %v", img.image)
	}
	defer func() {
		if err != nil {
			_ = img.unlock()
		}
	}()

	out, err := exec.Command(DrpRbdBinPath, "map", img.image).Output() //nolint: gas
	if err != nil {
		log.Errorf(err.Error())
		return "", fmt.Errorf("failed to map the image %v", img.image)
	}

	return strings.TrimSpace(string(out)), nil
}

func (img *RbdImage) unmapDevice() error {
	b, err := img.IsMounted()
	if err != nil {
		log.Errorf(err.Error())
		return fmt.Errorf("failed to determine if image %v is currently mounted", img.image)
	}

	if b {
		return fmt.Errorf("cannot unmap image %v because it is currently mounted", img.image)
	}

	b, err = img.IsMapped()
	if err != nil {
		log.Errorf(err.Error())
		return fmt.Errorf("failed to determine if image %v is mapped", img.image)
	}

	if b {
		err = exec.Command(DrpRbdBinPath, "unmap", img.image).Run() //nolint: gas
		if err != nil {
			log.Errorf(err.Error())

			return fmt.Errorf("error while trying to unmap the image %v", img.image)
		}
	}

	err = img.unlock()
	if err != nil {
		log.Errorf(err.Error())
		if b {
			return fmt.Errorf("error unlocking image %v which was not mapped", img.image)
		}
		return fmt.Errorf("error unlocking image %v", img.image)
	}

	return nil
}

//Remove removes the rbd image from ceph
func (img *RbdImage) Remove() error {
	b, err := img.IsMounted()
	if err != nil {
		log.Errorf(err.Error())
		return fmt.Errorf("error trying to determine if image %v is currently mounted", img.image)
	}

	if b {
		return fmt.Errorf("cannot remove image %v because it is currently mounted", img.image)
	}

	b, err = img.IsMapped()
	if err != nil {
		log.Errorf(err.Error())
		return fmt.Errorf("error trying to determine if image %v is mapped", img.image)
	}

	if b {
		return fmt.Errorf("cannot remove image %v because it is currently mapped", img.image)
	}

	b, err = img.IsLocked()
	if err != nil {
		log.Errorf(err.Error())
		return fmt.Errorf("error tyring to determine if image %v is locked", img.image)
	}

	if b {
		return fmt.Errorf("cannot remove image %v because it is currently locked", img.image)
	}

	err = exec.Command(DrpRbdBinPath, "remove", img.image).Run() //nolint: gas
	if err != nil {
		log.Errorf("error while trying to remove image %v", img.image)
		return err
	}

	return nil
}

//GetMapping returns the mapping for this image
func (img *RbdImage) GetMapping() (map[string]string, error) {
	mappings, err := GetMappings(img.PoolName())
	if err != nil {
		log.Errorf(err.Error())
		return nil, fmt.Errorf("error getting rbd mappings")
	}

	for _, v := range mappings {
		if img.image == v["pool"]+"/"+v["name"] {
			return v, nil
		}
	}

	return nil, fmt.Errorf("image %v doesn't seem to be mapped", img.image)
}

func (img *RbdImage) lock(refresh int) error {
	lock, err := AcquireLock(img, refresh)
	if err != nil {
		log.Errorf(err.Error())
		return fmt.Errorf("error while acquiring a lock for image %v with refresh %v", img.image, refresh)
	}

	img.activeLock = lock

	return nil
}

func (img *RbdImage) unlock() error {
	if img.activeLock == nil {
		return nil
	}

	err := img.activeLock.release()
	if err != nil {
		log.Errorf(err.Error())
		return fmt.Errorf("error while releasing a lock for image %v", img.image)
	}

	img.activeLock = nil

	return nil
}

//Mount mounts the image
func (img *RbdImage) Mount(mountid string) (string, error) {
	b, err := img.IsMounted()
	if err != nil {
		log.Errorf(err.Error())
		return "", fmt.Errorf("error determining if image %v is already mounted", img.image)
	}

	if b {
		var mp string
		mp, err = img.GetMountPoint()
		if err != nil {
			log.Errorf(err.Error())
			return "", fmt.Errorf("device for image %v is already mounted, failed to get the mountpoint", img.image)
		}
		img.users.add(mountid)
		return mp, nil
	}

	mp := os.Getenv("DRB_VOLUME_DIR")
	if mp == "" {
		mp = "/var/lib/docker-volumes/rbd"
	}
	mp += "/" + img.image

	//TODO: explore required directory perms
	err = os.MkdirAll(mp, 0755) //nolint: gas
	if err != nil {
		log.Errorf(err.Error())
		return mp, fmt.Errorf("error creating new mount point directory at %v", mp)
	}

	dev, err := img.mapDevice()
	if err != nil {
		log.Errorf(err.Error())
		return mp, fmt.Errorf("error mapping image %v to device", img.image)
	}
	defer func() {
		if err != nil {
			_ = img.unmapDevice()
		}
	}()

	out, err := exec.Command("blkid", "-s", "TYPE", "-o", "value", dev).Output() //nolint: gas
	if err != nil {
		log.Errorf(err.Error())
		return dev, fmt.Errorf("error running blkid to determine the filesystem type of image %v on dev %v", img.image, dev)
	}

	fs := strings.TrimSpace(string(out))

	log.Infof("mounting device %v to %v as %v filesystem", dev, mp, fs)
	err = syscall.Mount(dev, mp, fs, syscall.MS_NOATIME, "")
	if err != nil {
		log.Errorf(err.Error())
		return mp, fmt.Errorf("error while trying to mount device %v", dev)
	}

	img.users.add(mountid)
	return mp, nil
}

// Unmount refers to a docker unmount request. It SHOULD do more than just the syscall unmount
// like validate no containers are using it, etc...
func (img *RbdImage) Unmount(mountid string) error {
	err := img.users.reconcile(img.image)
	if err != nil {
		log.WithError(err).WithField("image", img.image).WithField("mountid", mountid).Error("failed to reconcile image users")
	}
	img.users.remove(mountid)

	if img.users.len() > 0 {
		log.Debugf("%v users still using the image %v", img.users.len(), img.image)
		return nil
	}

	var b bool
	b, err = img.IsMounted()
	if err != nil {
		log.Errorf(err.Error())
		return fmt.Errorf("error determining if image %v is mounted", img.image)
	}

	if b {
		var mp string
		mp, err = img.GetMountPoint()
		if err != nil {
			log.Errorf(err.Error())
			return fmt.Errorf("error determining the path of device for image %v", img.image)
		}

		err = syscall.Unmount(mp, 0)
		if err != nil {
			log.Errorf(err.Error())
			return fmt.Errorf("error while trying to unmount image %v from path %v", img.image, mp)
		}
	}

	b, err = img.IsMapped()
	if err != nil {
		log.Errorf(err.Error())
		return fmt.Errorf("error determining if image %v is mapped", img.image)
	}

	if b {
		err = img.unmapDevice()
		if err != nil {
			log.Errorf(err.Error())
			return fmt.Errorf("error unmapping image %v", img.image)
		}
	}

	// This is necessary in case a device is not mapped, but still locked for some reason.
	err = img.unlock()
	if err != nil {
		log.Errorf(err.Error())
		return fmt.Errorf("error unlocking image %v", img.image)
	}

	log.Debugf("image %v successfully unmounted", img.image)
	return nil
}

//GetAllLocks returns all the locks for this image
func (img *RbdImage) GetAllLocks() (map[string]map[string]string, error) {
	bytes, err := exec.Command(DrpRbdBinPath, "lock", "list", "--format", "json", img.image).Output() //nolint: gas
	if err != nil {
		log.Errorf(err.Error())
		return nil, fmt.Errorf("failed to get locks for image %v", img.image)
	}

	var locks map[string]map[string]string
	err = json.Unmarshal(bytes, &locks)
	if err != nil {
		log.Errorf(err.Error())
		return nil, fmt.Errorf("failed to unmarshal json: %v", string(bytes))
	}

	return locks, nil
}

//GetValidLocks returns only non-expired locks
func (img *RbdImage) GetValidLocks() (map[string]map[string]string, error) {
	locks, err := img.GetAllLocks()
	if err != nil {
		log.Errorf(err.Error())
		return nil, fmt.Errorf("error while getting locks for image %v", img.image)
	}

	vlocks := make(map[string]map[string]string)

	for k, v := range locks {
		lock := strings.Split(k, ",")

		var t time.Time
		t, err = time.Parse(time.RFC3339Nano, lock[1])
		if err != nil {
			log.Warningf(err.Error())
			log.Warningf("error while parsing time from lock id %v", k)
			continue
		}

		if time.Now().Before(t) {
			vlocks[k] = v
		}
	}

	return vlocks, nil
}

//IsLocked returns true if this image is locked
func (img *RbdImage) IsLocked() (bool, error) {
	locks, err := img.GetValidLocks()
	if err != nil {
		log.Errorf(err.Error())
		return false, fmt.Errorf("error while getting valid locks for image %v", img.image)
	}

	if len(locks) <= 0 {
		return false, nil
	}

	hn, err := os.Hostname()
	if err != nil {
		log.WithError(err).Error("error getting my hostname")
	}

	for k := range locks {
		lock := strings.Split(k, ",")
		if lock[0] != hn {
			return true, nil
		}
	}

	return false, nil
}

//EmergencyUnmap should be called only if we discover a mapped image that is locked by someone else
func (img *RbdImage) EmergencyUnmap(containerid string) error {
	dev, err := img.GetDevice()
	if err != nil {
		log.Errorf(err.Error())
		return fmt.Errorf("error getting image %v mapping", img.image)
	}

	if containerid != "" {
		log.Info("killing container with id %v", containerid)
		err = exec.Command("docker", "kill", containerid).Run() //nolint: gas
		if err != nil {
			log.WithError(err).Warningf("error killing container %v", containerid)
		}
	}

	log.Infof("killing all processes accessing %v", dev)
	err = exec.Command("sh", "-c", fmt.Sprintf("kill -9 $(lsof -t %v)", dev)).Run() //nolint: gas
	if err != nil {
		log.WithError(err).Error("error killing all processes accessing the device")
	}

	mp, err := img.GetMountPoint()
	if err != nil {
		log.WithError(err).WithField("image", img.image).Error("error getting the mountpoint")
	}

	if mp != "" {
		log.WithField("image", img.image).Info("attempting emergency unmount")
		err = syscall.Unmount(mp, 0)
		if err != nil {
			log.WithError(err).WithField("image", img.image).Error("error unmounting")
		}
	}

	log.Infof("attempting unmap of image %v", img.image)
	err = exec.Command(DrpRbdBinPath, "unmap", img.image).Run() //nolint: gas
	if err != nil {
		log.Errorf(err.Error())
		return fmt.Errorf("error while trying to unmap the image %v", img.image)
	}

	return nil
}

func (img *RbdImage) reapLocks() error {
	locks, err := img.GetAllLocks()
	if err != nil {
		log.Errorf(err.Error())
		return fmt.Errorf("error while getting locks for image %v", img.image)
	}

	for k, v := range locks {
		lock := strings.Split(k, ",")

		var t time.Time
		t, err = time.Parse(time.RFC3339Nano, lock[1])
		if err != nil {
			log.WithError(err).WithField("lock id", k).Warning("error while parsing time from lock id")
			continue
		}

		if time.Now().After(t) {
			log.WithFields(log.Fields{
				"lock id": k,
				"locker":  v["locker"],
				"address": v["address"],
			}).Info("reaping expired lock")
			err = img.removeLock(k, v["locker"])
			if err != nil {
				//wasn't previously handling an error here
				//TODO: make sure we shouldn't return err
				log.WithError(err).WithField("lockid", k).Error("error removing lock")
			}
		}
	}

	return nil
}

func (img *RbdImage) removeLock(id, locker string) error {
	err := exec.Command(DrpRbdBinPath, "lock", "rm", img.image, id, locker).Run() //nolint: gas
	if err != nil {
		log.Errorf(err.Error())
		return fmt.Errorf("error removing lock from image %v with id %v", img.image, id)
	}

	return nil
}

//GetCephLockExpiration returns the time of lock expiration of this image
func (img *RbdImage) GetCephLockExpiration() (time.Time, error) {
	exp := time.Now()

	locks, err := img.GetValidLocks()
	if err != nil {
		log.Error(err.Error())
		return exp, fmt.Errorf("error getting valid locks for image %v", img.image)
	}

	err = fmt.Errorf("no valid locks found for image %v", img.image)
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

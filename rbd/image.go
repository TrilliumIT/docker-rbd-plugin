package rbddriver

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"syscall"

	log "github.com/sirupsen/logrus"
)

//RbdImage represents a ceph rbd
type RbdImage struct {
	image string
	users *rbdUsers
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
			log.Errorf("detected error after creating image %v, removing", image)
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
	b, err := img.IsMapped()
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
		return dev, nil
	}

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

	log.Debugf("image %v successfully unmounted", img.image)
	return nil
}

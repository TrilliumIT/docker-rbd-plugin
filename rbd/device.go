package rbddriver

import (
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"syscall"

	log "github.com/Sirupsen/logrus"
)

type rbdDevice struct {
	name string
}

func newRbdDevice(name string) (*rbdDevice, error) {
	return &rbdDevice{name: name}, nil
}

func (dev *rbdDevice) isMounted() (bool, error) {
	mounts, err := getMounts()
	if err != nil {
		log.Errorf(err.Error())
		return false, fmt.Errorf("Failed to get the system mounts.")
	}

	_, ok := mounts[dev.name]

	return ok, nil
}

func (dev *rbdDevice) getMountPoint() (string, error) {
	mounts, err := getMounts()
	if err != nil {
		log.Errorf(err.Error())
		return "", fmt.Errorf("Failed to get the system mounts.")
	}

	mnt, ok := mounts[dev.name]
	if !ok {
		return "", fmt.Errorf("Device %v is not mounted.", dev.name)
	}

	return mnt.mountpoint, nil
}

func getMounts() (map[string]*mount, error) {
	mounts := make(map[string]*mount)

	mf := os.Getenv("DRP_MOUNTS_FILE")
	if mf == "" {
		mf = "/proc/self/mounts"
	}

	bytes, err := ioutil.ReadFile(mf)
	if err != nil {
		log.Errorf(err.Error())
		return nil, fmt.Errorf("Failed to read mounts file at %v.", mf)
	}

	lines := strings.Split(string(bytes), "\n")
	for _, line := range lines {
		if strings.TrimSpace(line) != "" {
			attrs := strings.Split(line, " ")

			dump, err := strconv.Atoi(attrs[4])
			if err != nil {
				dump = 0
			}
			fsck_order, err := strconv.Atoi(attrs[5])
			if err != nil {
				fsck_order = 0
			}

			mounts[attrs[0]] = &mount{
				device:     attrs[0],
				mountpoint: attrs[1],
				fstype:     attrs[2],
				options:    attrs[3],
				dump:       dump == 1,
				fsck_order: fsck_order,
			}
		}
	}

	return mounts, nil
}

func (dev *rbdDevice) mount(fstype, path string) error {
	b, err := dev.isMounted()
	if err != nil {
		log.Errorf(err.Error())
		return fmt.Errorf("Error determining if device %v is already mounted.", dev.name)
	}

	if b {
		return fmt.Errorf("Device %v is already mounted.", dev.name)
	}

	err = syscall.Mount(dev.name, path, fstype, 0, "")
	if err != nil {
		log.Errorf(err.Error())
		return fmt.Errorf("Error while trying to mount device %v.", dev.name)
	}

	return nil
}

func (dev *rbdDevice) unmount() error {
	b, err := dev.isMounted()
	if err != nil {
		log.Errorf(err.Error())
		return fmt.Errorf("Error determining if device %v is mounted.", dev.name)
	}

	if !b {
		return fmt.Errorf("Device %v is not mounted.", dev.name)
	}

	path, err := dev.getMountPoint()
	if err != nil {
		log.Errorf(err.Error())
		return fmt.Errorf("Error determining the path of device %v.", dev.name)
	}

	err = syscall.Unmount(path, 0)
	if err != nil {
		log.Errorf(err.Error())
		return fmt.Errorf("Error while trying to unmount device %v from path %v.", dev.name, path)
	}

	return nil
}

type mount struct {
	device     string
	mountpoint string
	fstype     string
	options    string
	dump       bool
	fsck_order int
}

package rbddriver

import "fmt"

type rbdDevice struct {
	name string
}

func newRbdDevice(name string) (*rbdDevice, error) {
	return &rbdDevice{name: name}, nil
}

func (dev *rbdDevice) isMounted() (bool, error) {
	return false, nil
}

func (dev *rbdDevice) getMountPoint() (string, error) {
	b, e := dev.isMounted()
	if e != nil {
		return "", fmt.Errorf("Failed to check if device %v is mounted.", dev.name)
	}

	if !b {
		return "", fmt.Errorf("Device %v is not mounted, cannot retrieve the mount point.", dev.name)
	}

	//TODO: actually get and return the mount point
	return "", nil
}

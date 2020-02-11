package rbd

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"

	log "github.com/sirupsen/logrus"
)

const (
	//DrpRbdBinPath is the default path of the rbd program
	DrpRbdBinPath = "/usr/bin/rbd"
)

//RBD represents a ceph rbd
type RBD struct {
	Pool     string   `json:"pool"`
	Name     string   `json:"name"`
	Size     int64    `json:"size"`
	Features []string `json:"features"`
	mutex    *sync.Mutex
}

var rbdMutexesMutex = &sync.RWMutex{}
var rbdMutexes = make(map[string]*sync.Mutex)

// RBDName returns the name in the format pool/name
func (rbd *RBD) RBDName() string {
	return rbd.Pool + "/" + rbd.Name
}

func (rbd *RBD) log() *log.Entry {
	return log.WithField("name", rbd.RBDName())
}

// ErrNoRBD is returned when an rbd does not exist
var ErrNoRBD = errors.New("rbd does not exist")

//GetRBD loads an existing rbd image from ceph and returns it
func GetRBD(rbdName string) (*RBD, error) {
	mutex := getMutex(rbdName)
	mutex.Lock()
	rbd, err := getRBD(rbdName, mutex)
	if err != nil {
		mutex.Unlock()
	}
	return rbd, err
}

func getRBD(rbdName string, mutex *sync.Mutex) (*RBD, error) {
	bytes, err := exec.Command(DrpRbdBinPath, "info", "--format", "json", rbdName).Output() //nolint: gas
	if err != nil {
		exitErr, isExitErr := err.(*exec.ExitError)
		if isExitErr && exitErr.ExitCode() == 2 {
			return nil, fmt.Errorf("%v: %w", rbdName, ErrNoRBD)
		}
		return nil, fmt.Errorf("error loading %v: %w", rbdName, err)
	}

	rbd := &RBD{Pool: strings.Split(rbdName, "/")[0], mutex: mutex}

	err = json.Unmarshal(bytes, rbd)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling %v: %w", rbdName, err)
	}

	//TODO get mapped info

	return rbd, nil
}

//CreateRBD creates a new rbd image in ceph
func CreateRBD(rbdName, size string) (*RBD, error) {
	mutex := getMutex(rbdName)
	mutex.Lock()

	log.Debugf("executing: rbd create %v --size %v --rbdName-feature exclusive-lock", rbdName, size)
	err := exec.Command(DrpRbdBinPath, "create", rbdName, "--size", size, "--rbdName-feature", "exclusive-lock").Run() //nolint: gas
	if err != nil {
		exitErr, isExitErr := err.(*exec.ExitError)
		if isExitErr && exitErr.ExitCode() == 17 {
			log.Debugf("rbd %v already exists", rbdName)
		} else {
			return nil, fmt.Errorf("error trying to create the rbdName %v: %w", rbdName, err)
		}
	}

	return getRBD(rbdName, mutex)
}

func getMutex(rbdName string) *sync.Mutex {
	rbdMutexesMutex.RLock()
	mutex := rbdMutexes[rbdName]
	if mutex == nil {
		rbdMutexesMutex.RUnlock()
		rbdMutexesMutex.Lock()
		mutex = rbdMutexes[rbdName]
		if mutex == nil {
			mutex = &sync.Mutex{}
			rbdMutexes[rbdName] = mutex
		}
		rbdMutexesMutex.Unlock()
	} else {
		rbdMutexesMutex.RUnlock()
	}
	return mutex
}

func (rbd *RBD) Unlock() {
	rbd.mutex.Unlock()
}

// MKFS formats the rbd, mapping if rqeuired and unmapping if it was mapped
func (rbd *RBD) MKFS(fs string) error {
	dev, mapped, err := rbd.mapAndEnableLocks()
	if err != nil {
		return err
	}

	err = exec.Command("mkfs."+fs, dev).Run() //nolint: gas
	if err != nil {
		return fmt.Errorf("failed to create filesystem on %v for %v: %w", dev, rbd.RBDName(), err)
	}

	if mapped {
		return rbd.UnMap()
	}

	return nil
}

//Device returns the device the rbd is mapped to or the empty string if it is not mapped
func (rbd *RBD) device() (string, error) {
	mappings, err := ShowMapped()
	if err != nil {
		return "", err
	}

	for _, m := range mappings {
		if rbd.Pool == m.Pool && rbd.Name == m.Name {
			return m.Device, nil
		}
	}

	return "", nil
}

// ErrErrExclusiveLockNotEnabled is returned when an rbd volume does not have exclusive-locks feature enabled
var ErrExclusiveLockNotEnabled = errors.New("exclusive-lock not enabled")

// EErrExclusiveLockTaken is returned when this client cannot get an exclusive-lock
var ErrExclusiveLockTaken = errors.New("exclusive-lock is held by another client")

// Map maps an rbd device and returns the device or returns an existing device if already mapped
func (rbd *RBD) Map() (string, error) {
	dev, _, err := rbd.mapAndEnableLocks()
	return dev, err
}

func (rbd *RBD) mapAndEnableLocks() (string, bool, error) {
	dev, mapped, err := rbd.mapRBD()
	if err != nil && errors.Is(err, ErrExclusiveLockNotEnabled) {
		rbd.log().Debug("exclusive lock not enabled, enabling now")
		err = rbd.EnableExclusiveLocks()
		if errors.Is(err, ErrExclusiveLockAlreadyEnabled) {
			err = nil
		}
		if err == nil {
			dev, mapped, err = rbd.mapRBD()
		}
	}
	return dev, mapped, err
}

func (rbd *RBD) mapRBD() (string, bool, error) {
	dev, err := rbd.device()
	if err != nil {
		return "", false, fmt.Errorf("failed to detrimine if %v is already mapped: %w", rbd.RBDName(), err)
	}

	if dev != "" {
		return dev, false, nil
	}

	out, err := exec.Command(DrpRbdBinPath, "map", "--exclusive", rbd.RBDName()).Output() //nolint: gas
	exitErr, isExitErr := err.(*exec.ExitError)
	if isExitErr && exitErr.ExitCode() == 22 {
		return "", false, ErrExclusiveLockNotEnabled
	}
	if isExitErr && exitErr.ExitCode() == 30 {
		return "", false, ErrExclusiveLockTaken
	}
	if err != nil {
		return "", false, fmt.Errorf("failed to map %v: %w", rbd.RBDName(), err)
	}

	return strings.TrimSpace(string(out)), true, nil
}

var ErrExclusiveLockAlreadyEnabled = errors.New("exclusive-lock already enabled")

func (rbd *RBD) EnableExclusiveLocks() error {
	err := exec.Command(DrpRbdBinPath, "feature", "enable", rbd.RBDName(), "exclusive-lock").Run() //nolint: gas
	exitErr, isExitErr := err.(*exec.ExitError)
	if isExitErr && exitErr.ExitCode() == 22 {
		return ErrExclusiveLockAlreadyEnabled
	}
	if err != nil {
		return fmt.Errorf("failed to enable exclusive-lock on %v: %w", rbd.RBDName(), err)
	}
	return nil
}

func (rbd *RBD) UnMap() error {
	err := exec.Command(DrpRbdBinPath, "unmap", rbd.RBDName()).Run() //nolint: gas
	exitErr, isExitErr := err.(*exec.ExitError)
	if isExitErr && exitErr.ExitCode() == 16 {
		return fmt.Errorf("error unmapping (is the device mounted elsewhere, or in other containers?): %w", err)
	}
	if err != nil {
		return fmt.Errorf("error unmapping %v: %w", rbd.RBDName(), err)
	}
	return nil
}

//Remove removes the rbd image from ceph
func (rbd *RBD) Remove() error {
	err := exec.Command(DrpRbdBinPath, "remove", rbd.RBDName()).Run() //nolint: gas
	if err != nil {
		return fmt.Errorf("error removing %v: %w", rbd.RBDName(), err)
	}
	return nil
}

func (rbd *RBD) GetMounts() ([]*Mount, error) {
	return rbd.getMounts(GetMounts)
}

func (rbd *RBD) getMounts(getMounts func(string) ([]*Mount, error)) ([]*Mount, error) {
	dev, err := rbd.device()
	if err != nil {
		return nil, fmt.Errorf("failed to get device for %v: %w", rbd.RBDName(), err)
	}
	if dev == "" {
		return []*Mount{}, nil
	}

	mounts, err := getMounts(dev)
	if err != nil {
		return nil, fmt.Errorf("failed to get the mounts for %v(%v): %w", dev, rbd.RBDName(), err)
	}

	return mounts, nil
}

// MountsIn gets mountpoint
func (rbd *RBD) IsMountedAt(mountpoint string) (bool, error) {
	mounts, err := rbd.GetMounts()
	if err != nil {
		return false, err
	}

	for _, mount := range mounts {
		if mount.MountPoint == mountpoint {
			return true, nil
		}
	}

	return false, nil
}

//Mount mounts the image
func (rbd *RBD) Mount(mountpoint string) (string, error) {
	alreadyMounted, err := rbd.IsMountedAt(mountpoint)
	if err != nil {
		return "", fmt.Errorf("failed to get determine if %v is already mounted at %v: %w", rbd.RBDName(), mountpoint, err)
	}

	if alreadyMounted {
		return mountpoint, nil
	}

	dev, err := rbd.Map()
	if err != nil {
		return "", fmt.Errorf("failed to get device for %v: %w", rbd.RBDName(), err)
	}

	out, err := exec.Command("blkid", "-s", "TYPE", "-o", "value", dev).Output() //nolint: gas
	if err != nil {
		return "", fmt.Errorf("error determining filesystem on %v: %w", dev, err)
	}
	fs := strings.TrimSpace(string(out))

	err = os.MkdirAll(mountpoint, 0755) //nolint: gas
	if err != nil {
		return "", fmt.Errorf("Error creating directory: %v: %w", mountpoint, err)
	}

	rbd.log().Infof("mounting device %v to %v as %v filesystem", dev, mountpoint, fs)
	err = syscall.Mount(dev, mountpoint, fs, syscall.MS_NOATIME, "")
	if err != nil {
		return "", fmt.Errorf("error mounting %v to %v as %v filesystem: %w", dev, mountpoint, fs, err)
	}

	return mountpoint, nil
}

// Unmount unmounts the from mountpoint
func (rbd *RBD) Unmount(mountpoint string) error {
	mounted, err := rbd.IsMountedAt(mountpoint)
	if err != nil {
		return err
	}
	if mounted {
		err = syscall.Unmount(mountpoint, 0)
		if err != nil {
			return fmt.Errorf("error unmounting %v from %v: %w", rbd.RBDName(), mountpoint, err)
		}
	}
	return nil
}

// UnmountAndUnmapd unmounts the rbd and unmaps it
func (rbd *RBD) UnmountAndUnmap(mountpoint string) error {
	err := rbd.Unmount(mountpoint)
	if err != nil {
		return fmt.Errorf("error unmounting: %w", err)
	}

	err = rbd.UnMap()
	if err != nil {
		return fmt.Errorf("error unmapping: %w", err)
	}

	return nil
}

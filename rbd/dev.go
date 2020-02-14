package rbd

import (
	"errors"
)

// Dev is an rbd device, a snapshot or an image
type Dev interface {
	FullName() string
	ImageName() string
	Name() string
	Pool() *Pool
	IsMountedAt(string) (bool, error)
	Map(...string) (string, error)
	Mount(string, uintptr) error
	Unmount(string) error
	Unmap() error
	MapAndMount(string, uintptr, ...string) error
	UnmountAndUnmap(string) error
	Remove() error
}

func device(d Dev) (string, error) {
	mapped, err := mappedNBDs()
	if err != nil {
		return "", err
	}
	for _, m := range mapped {
		if m.Snapshot == "-" && m.Name == d.Name() && m.Pool == d.Pool().Name() {
			return m.Device, nil
		}
	}
	return "", nil
}

func devIsMountedAt(d Dev, mountPoint string) (bool, error) {
	blk, err := device(d)
	if err != nil || blk == "" {
		return false, err
	}
	return isMountedAt(blk, mountPoint)
}

// ErrErrExclusiveLockNotEnabled is returned when an rbd volume does not have exclusive-locks feature enabled
var ErrExclusiveLockNotEnabled = errors.New("exclusive-lock not enabled")

// EErrExclusiveLockTaken is returned when this client cannot get an exclusive-lock
var ErrExclusiveLockTaken = errors.New("exclusive-lock is held by another client")

var mapErrors = map[int]error{
	22: ErrExclusiveLockNotEnabled,
	30: ErrExclusiveLockTaken,
}

func devMap(d Dev, args ...string) (string, error) {
	nbd, err := device(d)
	if err != nil || nbd != "" {
		return nbd, err
	}
	args = append([]string{"nbd", "map", "--image", d.ImageName()}, args...)
	args = d.Pool().cmdArgs(args...)
	return cmdOut(mapErrors, args...)
}

func devCmdArgs(d Dev, args ...string) []string {
	args = append([]string{"--image", d.ImageName()}, args...)
	return d.Pool().cmdArgs(args...)
}

func devMapAndMount(d Dev, mountPoint string, flags uintptr, mapF func() (string, error)) error {
	err := d.Mount(mountPoint, flags)
	if errors.Is(err, ErrNotMapped) {
		blk, err := mapF()
		if err != nil {
			return err
		}
		return mount(blk, mountPoint, flags)
	}
	return err
}

var ErrNotMapped = errors.New("image not mapped")

func devMount(d Dev, mountPoint string, flags uintptr) error {
	blk, err := device(d)
	if err != nil {
		return err
	}
	if blk == "" {
		return ErrNotMapped
	}
	return mount(blk, mountPoint, flags)
}

func devUnmount(d Dev, mountPoint string) error {
	blk, err := device(d)
	if err != nil || blk == "" {
		return err
	}
	return unmount(blk, mountPoint)
}

// devUnmountAndUnmap safely unmounts and unmaps checking for would-be orphan mounts first
func devUnmountAndUnmap(d Dev, mountPoint string) error {
	blk, err := device(d)
	if err != nil || blk == "" {
		return err
	}
	if err = isMountedElsewhere(blk, mountPoint); err != nil {
		return err
	}
	if err = unmount(blk, mountPoint); err != nil {
		return err
	}
	return unmap(blk)
}

var ErrDeviceBusy = errors.New("device busy")
var unmapErrors = map[int]error{
	16: ErrDeviceBusy,
}

func unmap(blk string) error {
	return cmdRun(unmapErrors, "nbd", "unmap", blk)
}

func devUnmap(d Dev) error {
	blk, err := device(d)
	if err != nil || blk == "" {
		return err
	}
	return unmap(blk)
}

func devRemove(d Dev) error {
	return cmdRun(nil, devCmdArgs(d, "remove")...)
}

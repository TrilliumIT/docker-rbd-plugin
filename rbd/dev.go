package rbd

import (
	"encoding/json"
	"errors"
	"strings"
	"time"
)

// Dev is an rbd device, a snapshot or an image
type Dev interface {
	FullName() string
	ImageName() string
	Name() string
	Pool() *Pool
	Info() (*DevInfo, error)
	IsMountedAt(string) (bool, error)
	Map(...string) (string, error)
	Mount(string, string, uintptr, string) error
	Unmount(string) error
	Unmap() error
	MapAndMount(string, string, uintptr, string, ...string) error
	UnmountAndUnmap(string) error
	Remove() error
	FileSystem() (string, error)
	cmdArgs(...string) []string
}

func devFullName(d Dev) string {
	return d.Pool().Name() + "/" + d.ImageName()
}

func device(d Dev) (string, error) {
	mapped, err := mappedNBDs()
	if err != nil {
		return "", err
	}
	for _, m := range mapped {
		switch v := d.(type) {
		case *Image:
			if m.Snapshot == "-" && m.Name == v.Name() && m.Pool == v.Pool().Name() {
				return m.Device, nil
			}
		case *Snapshot:
			if m.Snapshot == v.Name() && m.Name == v.Image().Name() && m.Pool == v.Pool().Name() {
				return m.Device, nil
			}
		}
	}
	return "", nil
}

var ErrNotMapped = errors.New("not mapped")

func mustDevice(d Dev) (string, error) {
	blk, err := device(d)
	if err == nil && blk == "" {
		err = ErrNotMapped
	}
	return blk, err
}

func devFileSystem(d Dev) (string, error) {
	blk, err := mustDevice(d)
	if err != nil {
		return blk, err
	}
	return getFs(blk)
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
	args = append([]string{"nbd", "map"}, args...)
	args = d.cmdArgs(args...)
	return cmdOut(mapErrors, args...)
}

func devMapAndMount(d Dev, mountPoint, fs string, flags uintptr, data string, mapF func() (string, error)) error {
	err := d.Mount(mountPoint, fs, flags, data)
	if errors.Is(err, ErrNotMapped) {
		blk, err := mapF()
		if err != nil {
			return err
		}
		return mount(blk, mountPoint, fs, flags, data)
	}
	return err
}

func devMount(d Dev, mountPoint, fs string, flags uintptr, data string) error {
	blk, err := mustDevice(d)
	if err != nil {
		return err
	}
	return mount(blk, mountPoint, fs, flags, data)
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

type DevInfo struct {
	Name            string          `json:"name"`
	Size            int64           `json:"size"`
	Objects         int             `json:"objects"`
	Order           int             `json:"order"`
	ObjectSize      int             `json:"object_size"`
	BlockNamePrefix string          `json:"block_name_prefix"`
	Format          int             `json:"format"`
	Features        []string        `json:"features"`
	Flags           []interface{}   `json:"flags"`
	CreateTimestamp CreateTimestamp `json:"create_timestamp"`
	Protected       bool            `json:"protected,string"`
}

type CreateTimestamp time.Time

func (j *CreateTimestamp) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), "\"")
	t, err := time.ParseInLocation(time.ANSIC, s, time.Local)
	if err != nil {
		return err
	}
	*j = CreateTimestamp(t)
	return nil
}

func (j CreateTimestamp) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Time(j).Format(time.ANSIC))
}

func devInfo(d Dev) (*DevInfo, error) {
	i := &DevInfo{}
	return i, cmdJSON(i, imageErrs, d.cmdArgs("info")...)
}

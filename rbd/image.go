package rbd

import (
	"errors"
)

// Image is an rbd image
type Image struct {
	name string
	pool *Pool
}

var _ Dev = (*Image)(nil) // compile check that Image satisfies  Dev

// ErrImageDoesNotExist is returned if an image does not exist
var ErrImageDoesNotExist = errors.New("image does not exist")

func getImage(pool *Pool, name string) *Image {
	img := &Image{name, pool}
	return img
}

// Name is the name
func (img *Image) Name() string {
	return img.name
}

// ImageName is the name in the format image@snapshot
func (img *Image) ImageName() string {
	return img.Name()
}

// FullName is the full name in the format pool/image@snapshot
func (img *Image) FullName() string {
	return devFullName(img)
}

// Pool is the pool this image lives on
func (img *Image) Pool() *Pool {
	return img.pool
}

func (img *Image) cmdArgs(args ...string) []string {
	args = append([]string{"--image", img.Name()}, args...)
	return img.Pool().cmdArgs(args...)
}

// IsMountedAt returns true if mounted at mountPoint
func (img *Image) IsMountedAt(mountPoint string) (bool, error) {
	return devIsMountedAt(img, mountPoint)
}

// Info returns information
func (img *Image) Info() (*DevInfo, error) {
	return devInfo(img)
}

// Map maps to an nbd device
func (img *Image) Map(args ...string) (string, error) {
	return devMap(img, args...)
}

// MapExclusive maps to an nbd device using the exclusive option
func (img *Image) MapExclusive(args ...string) (string, error) {
	args = append([]string{"--exclusive"}, args...)
	blk, err := devMap(img, args...)
	if errors.Is(err, ErrExclusiveLockNotEnabled) {
		err = img.EnableFeatures("exclusive-locks")
		if err != nil {
			return "", err
		}
		return devMap(img, args...)
	}
	return blk, err
}

// ErrFeatureAlreadyEnabled is returned when enabling a feature that is already enabled
var ErrFeatureAlreadyEnabled = errors.New("feature already enabled")

var featureEnableErrMap = exitCodeToErrMap(map[int]error{22: ErrFeatureAlreadyEnabled})

// EnableFeatures enables features
func (img *Image) EnableFeatures(feature ...string) error {
	args := append([]string{"feature", "enable"}, img.cmdArgs(feature...)...)
	return cmdRun(featureEnableErrMap, args...)
}

// DisableFeatures disables features
func (img *Image) DisableFeatures(feature ...string) error {
	args := append([]string{"feature", "disable"}, feature...)
	args = img.cmdArgs(args...)
	return cmdRun(nil, args...)
}

// Mount mounts the device (must already be mapped)
func (img *Image) Mount(mountPoint, fs string, flags uintptr, data string) error {
	return devMount(img, mountPoint, fs, flags, data)
}

// MapAndMount mounts the device, mapping it first if necessary
func (img *Image) MapAndMount(mountPoint, fs string, flags uintptr, data string, args ...string) error {
	return devMapAndMount(img, mountPoint, fs, flags, data, func() (string, error) { return img.Map(args...) })
}

// MapAndMountExclusive mounts the device, mapping it exclusively first if necessary
func (img *Image) MapAndMountExclusive(mountPoint, fs string, flags uintptr, data string, args ...string) error {
	return devMapAndMount(img, mountPoint, fs, flags, data, func() (string, error) { return img.MapExclusive(args...) })
}

// Unmap unmapps the device
func (img *Image) Unmap() error {
	return devUnmap(img)
}

// Unmount unmounts the device
func (img *Image) Unmount(mountPoint string) error {
	return devUnmount(img, mountPoint)
}

// UnmountAndUnmap unmounts and unmaps the device
func (img *Image) UnmountAndUnmap(mountPoint string) error {
	return devUnmountAndUnmap(img, mountPoint)
}

// Remove deletes the device from the pool
func (img *Image) Remove() error {
	return cmdRun(nil, img.cmdArgs("remove", "--no-progress")...)
}

func (img *Image) getSnapshot(name string) *Snapshot {
	return getSnapshot(img, name)
}

// GetSnapshot gets an existing snapshot of the image
func (img *Image) GetSnapshot(name string) (*Snapshot, error) {
	snap := img.getSnapshot(name)
	_, err := snap.Info()
	return snap, err
}

// CreateSnapshot creates a snapshot of the image
func (img *Image) CreateSnapshot(name string) (*Snapshot, error) {
	args := img.cmdArgs("snap", "create", "--snap", name)
	err := cmdRun(createErrs, args...)
	if err != nil && !errors.Is(err, ErrAlreadyExists) {
		return nil, err
	}
	return img.getSnapshot(name), err
}

// CreateConsistentSnapshot creates a snapshot of the image, freezing the filesystem first for consistency
func (img *Image) CreateConsistentSnapshot(name string, onlyIfMapped bool) (*Snapshot, error) {
	blk, err := device(img)
	if err != nil {
		return nil, err
	}
	if onlyIfMapped && blk == "" {
		return nil, ErrNotMapped
	}
	if blk != "" {
		unfreeze, err := fsFreezeBlk(blk)
		defer unfreeze()
		if err != nil {
			return nil, err
		}
	}
	return img.CreateSnapshot(name)
}

// Snapshots returns all existing snapshots of the image
func (img *Image) Snapshots() ([]*Snapshot, error) {
	args := img.cmdArgs("snap", "list")
	snaps := []*snapshotListEntry{}
	err := cmdJSON(&snaps, nil, args...)
	if err != nil {
		return nil, err
	}
	r := make([]*Snapshot, 0, len(snaps))
	for _, s := range snaps {
		r = append(r, img.getSnapshot(s.Name))
	}
	return r, nil
}

// FileSystem returns the filesystem of the image
func (img *Image) FileSystem() (string, error) {
	return devFileSystem(img)
}

// LockInfo is an rbd lock
type LockInfo struct {
	Locker  string
	address string
}

func (img *Image) GetLocks() (map[string]*LockInfo, error) {
	args := img.cmdArgs("lock", "list")
	locks := make(map[string]*LockInfo)
	return locks, cmdJSON(&locks, nil, args...)
}

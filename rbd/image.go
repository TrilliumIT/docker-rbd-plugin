package rbd

import (
	"errors"
)

type Image struct {
	name string
	pool *Pool
}

var _ Dev = (*Image)(nil) // compile check that Image satisfies  Dev

var ErrImageDoesNotExist = errors.New("image does not exist")

func getImage(pool *Pool, name string) *Image {
	img := &Image{name, pool}
	return img
}

func (img *Image) Name() string {
	return img.name
}

func (img *Image) ImageName() string {
	return img.Name()
}

func (img *Image) FullName() string {
	return devFullName(img)
}

func (img *Image) Pool() *Pool {
	return img.pool
}

func (img *Image) cmdArgs(args ...string) []string {
	args = append([]string{"--image", img.Name()}, args...)
	return img.Pool().cmdArgs(args...)
}

func (img *Image) IsMountedAt(mountPoint string) (bool, error) {
	return devIsMountedAt(img, mountPoint)
}

func (img *Image) Info() (*DevInfo, error) {
	return devInfo(img)
}

func (img *Image) Map(args ...string) (string, error) {
	return devMap(img, args...)
}

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

func (img *Image) EnableFeatures(feature ...string) error {
	args := append([]string{"feature", "enable"}, feature...)
	args = img.cmdArgs(args...)
	return cmdRun(nil, args...)
}

func (img *Image) DisableFeatures(feature ...string) error {
	args := append([]string{"feature", "disable"}, feature...)
	args = img.cmdArgs(args...)
	return cmdRun(nil, args...)
}

func (img *Image) Mount(mountPoint, fs string, flags uintptr, data string) error {
	return devMount(img, mountPoint, fs, flags, data)
}

func (img *Image) MapAndMount(mountPoint, fs string, flags uintptr, data string, args ...string) error {
	return devMapAndMount(img, mountPoint, fs, flags, data, func() (string, error) { return img.Map(args...) })
}

func (img *Image) MapAndMountExclusive(mountPoint, fs string, flags uintptr, data string, args ...string) error {
	return devMapAndMount(img, mountPoint, fs, flags, data, func() (string, error) { return img.MapExclusive(args...) })
}

func (img *Image) Unmap() error {
	return devUnmap(img)
}

func (img *Image) Unmount(mountPoint string) error {
	return devUnmount(img, mountPoint)
}

func (img *Image) UnmountAndUnmap(mountPoint string) error {
	return devUnmountAndUnmap(img, mountPoint)
}

func (img *Image) Remove() error {
	return cmdRun(nil, img.cmdArgs("remove", "--no-progress")...)
}

func (img *Image) getSnapshot(name string) *Snapshot {
	return getSnapshot(img, name)
}

func (img *Image) GetSnapshot(name string) (*Snapshot, error) {
	snap := img.getSnapshot(name)
	_, err := snap.Info()
	return snap, err
}

func (img *Image) CreateSnapshot(name string) (*Snapshot, error) {
	args := img.cmdArgs("snap", "create", "--snap", name)
	err := cmdRun(createErrs, args...)
	if err != nil && !errors.Is(err, ErrAlreadyExists) {
		return nil, err
	}
	return img.getSnapshot(name), err
}

func (img *Image) CreateConsistentSnapshot(name string, onlyIfMapped bool) (*Snapshot, error) {
	blk, err := device(img)
	if err != nil {
		return nil, err
	}
	if onlyIfMapped && blk == "" {
		return nil, ErrNotMapped
	}
	if blk != "" {
		unfreeze, err := FSFreezeBlk(blk)
		defer unfreeze()
		if err != nil {
			return nil, err
		}
	}
	return img.CreateSnapshot(name)
}

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

func (img *Image) FileSystem() (string, error) {
	return devFileSystem(img)
}

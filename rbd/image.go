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
	return img.pool.Name() + "/" + img.Name()
}

func (img *Image) Pool() *Pool {
	return img.pool
}

func (img *Image) IsMountedAt(mountPoint string) (bool, error) {
	return devIsMountedAt(img, mountPoint)
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
	args = devCmdArgs(img, args...)
	return cmdRun(nil, args...)
}

func (img *Image) DisableFeatures(feature ...string) error {
	args := append([]string{"feature", "disable"}, feature...)
	args = devCmdArgs(img, args...)
	return cmdRun(nil, args...)
}

func (img *Image) Mount(mountPoint string, flags uintptr) error {
	return devMount(img, mountPoint, flags)
}

func (img *Image) MapAndMount(mountPoint string, flags uintptr, args ...string) error {
	return devMapAndMount(img, mountPoint, flags, func() (string, error) { return img.Map(args...) })
}

func (img *Image) MapAndMountExclusive(mountPoint string, flags uintptr, args ...string) error {
	return devMapAndMount(img, mountPoint, flags, func() (string, error) { return img.MapExclusive(args...) })
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
	return devRemove(img)
}

func (img *Image) getSnap(name string) *Snap {
	return getSnap(img, name)
}

package rbd

import "syscall"

type Snapshot struct {
	name  string
	image *Image
}

type snapshotListEntry struct {
	ID        int    `json:"id"`
	Name      string `json:"name"`
	Size      int64  `json:"size"`
	Timestamp string `json:"timestamp"`
}

func (snap *Snapshot) Image() *Image {
	return snap.image
}

const XFS_MOUNT_NORECOVERY uintptr = 1 << 10 // see xfs_mount.h

var _ Dev = (*Snapshot)(nil) // compile check that Image satisfies  Dev

func getSnapshot(img *Image, name string) *Snapshot {
	return &Snapshot{name, img}
}

func (snap *Snapshot) Name() string {
	return snap.name
}

func (snap *Snapshot) ImageName() string {
	return snap.image.Name() + "@" + snap.Name()
}

func (snap *Snapshot) FullName() string {
	return devFullName(snap)
}

func (snap *Snapshot) Pool() *Pool {
	return snap.image.Pool()
}

func (snap *Snapshot) Info() (*DevInfo, error) {
	return devInfo(snap)
}

func (snap *Snapshot) IsMountedAt(mountPoint string) (bool, error) {
	return devIsMountedAt(snap, mountPoint)
}

func (snap *Snapshot) Map(args ...string) (string, error) {
	args = append([]string{"--read-only"}, args...)
	return devMap(snap, args...)
}

func (snap *Snapshot) Mount(mountPoint, fs string, flags uintptr, data string) error {
	flags = flags & syscall.MS_RDONLY
	return devMount(snap, mountPoint, fs, flags, data)
}

func (snap *Snapshot) MapAndMount(mountPoint, fs string, flags uintptr, data string, args ...string) error {
	return devMapAndMount(snap, mountPoint, fs, flags, data, func() (string, error) { return snap.Map(args...) })
}

func (snap *Snapshot) Unmap() error {
	return devUnmap(snap)
}

func (snap *Snapshot) Unmount(mountPoint string) error {
	return devUnmount(snap, mountPoint)
}

func (snap *Snapshot) UnmountAndUnmap(mountPoint string) error {
	return devUnmountAndUnmap(snap, mountPoint)
}

func (snap *Snapshot) Remove() error {
	return devRemove(snap)
}

func (snap *Snapshot) FileSystem() (string, error) {
	return devFileSystem(snap)
}

package rbd

import "syscall"

// Snapshot is a snapshot
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

// Image is the image
func (snap *Snapshot) Image() *Image {
	return snap.image
}

var _ Dev = (*Snapshot)(nil) // compile check that Image satisfies  Dev

func getSnapshot(img *Image, name string) *Snapshot {
	return &Snapshot{name, img}
}

// Name is the snapshot name
func (snap *Snapshot) Name() string {
	return snap.name
}

// ImageName is the name in the format image@snapshot
func (snap *Snapshot) ImageName() string {
	return snap.image.Name() + "@" + snap.Name()
}

// FullName is the full name in the format pool/image@snapshot
func (snap *Snapshot) FullName() string {
	return devFullName(snap)
}

// Pool is the pool this image lives on
func (snap *Snapshot) Pool() *Pool {
	return snap.image.Pool()
}

// Device returns the nbd device that this image is mapped to
func (snap *Snapshot) Device() (string, error) {
	return device(snap)
}

func (snap *Snapshot) cmdArgs(args ...string) []string {
	args = append([]string{"--snap", snap.Name()}, args...)
	return snap.Image().cmdArgs(args...)
}

// Info returns info about this snapshot
func (snap *Snapshot) Info() (*DevInfo, error) {
	return devInfo(snap)
}

// IsMountedAt returns true if mounted at mountPoint
func (snap *Snapshot) IsMountedAt(mountPoint string) (bool, error) {
	return devIsMountedAt(snap, mountPoint)
}

// Map maps to an nbd device
func (snap *Snapshot) Map(args ...string) (string, error) {
	args = append([]string{"--read-only"}, args...)
	return devMap(snap, args...)
}

// Mount mounts the device (must already be mapped)
func (snap *Snapshot) Mount(mountPoint, fs string, flags uintptr, data string) error {
	flags = flags & syscall.MS_RDONLY
	return devMount(snap, mountPoint, fs, flags, data)
}

// MapAndMount mounts the device, mapping it first if necessary
func (snap *Snapshot) MapAndMount(mountPoint, fs string, flags uintptr, data string, args ...string) error {
	return devMapAndMount(snap, mountPoint, fs, flags, data, func() (string, error) { return snap.Map(args...) })
}

// Unmap unmapps the device
func (snap *Snapshot) Unmap() error {
	return devUnmap(snap)
}

// Unmount unmounts the device
func (snap *Snapshot) Unmount(mountPoint string) error {
	return devUnmount(snap, mountPoint)
}

// UnmountAndUnmap unmounts and unmaps the device
func (snap *Snapshot) UnmountAndUnmap(mountPoint string) error {
	return devUnmountAndUnmap(snap, mountPoint)
}

// Remove deletes the device from the pool
func (snap *Snapshot) Remove() error {
	return cmdRun(nil, snap.cmdArgs("snap", "remove", "--no-progress")...)
}

// FileSystem returns the filesystem of the image
func (snap *Snapshot) FileSystem() (string, error) {
	return devFileSystem(snap)
}

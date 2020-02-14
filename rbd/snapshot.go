package rbd

type Snap struct {
	name  string
	image *Image
}

var _ Dev = (*Snap)(nil) // compile check that Image satisfies  Dev

func getSnap(img *Image, name string) *Snap {
	return &Snap{name, img}
}

func (snap *Snap) Name() string {
	return snap.name
}

func (snap *Snap) ImageName() string {
	return snap.image.Name() + "@" + snap.Name()
}

func (snap *Snap) FullName() string {
	return snap.image.FullName() + "@" + snap.Name()
}

func (snap *Snap) Pool() *Pool {
	return snap.image.Pool()
}

func (snap *Snap) IsMountedAt(mountPoint string) (bool, error) {
	return devIsMountedAt(snap, mountPoint)
}

func (snap *Snap) Map(args ...string) (string, error) {
	return devMap(snap, args...)
}

func (snap *Snap) Mount(mountPoint string, flags uintptr) error {
	// TODO
	return nil
}

func (snap *Snap) MapAndMount(mountPoint string, flags uintptr, args ...string) error {
	_, err := snap.Map(args...)
	if err != nil {
		return err
	}
	return snap.Mount(mountPoint, flags)
}

func (snap *Snap) Unmap() error {
	// TODO
	return nil
}

func (snap *Snap) Unmount(mountPoint string) error {
	// TODO
	return nil
}

func (snap *Snap) UnmountAndUnmap(mountPoint string) error {
	// TODO
	return nil
}

func (snap *Snap) Remove() error {
	//TODO
	return nil
}

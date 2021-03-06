package rbd

import (
	"errors"
)

// Pool is an rbd pool
type Pool struct {
	name string
}

// Name is the pool name
func (pool *Pool) Name() string {
	return pool.name
}

// GetPool gets a pool object (does not verify pool exists)
func GetPool(name string) *Pool {
	return &Pool{name}
}

// ErrDoesNotExist is returned if the pool, image or snapshot does not exist
var ErrDoesNotExist = errors.New("does not exist")

func (pool *Pool) cmdArgs(args ...string) []string {
	return append([]string{"--pool", pool.name}, args...)
}

func (pool *Pool) getImage(name string) *Image {
	return getImage(pool, name)
}

var poolErrs = exitCodeToErrMap(map[int]error{2: ErrDoesNotExist})

// Images returns the rbd images
func (pool *Pool) Images() ([]*Image, error) {
	imgNames := []string{}
	err := cmdJSON(&imgNames, poolErrs, pool.cmdArgs("list")...)
	images := make([]*Image, 0, len(imgNames))
	for _, n := range imgNames {
		images = append(images, pool.getImage(n))
	}
	return images, err
}

func (pool *Pool) MappedImages() ([]*Image, error) {
	mappedNBDs, err := mappedNBDs()
	if err != nil {
		return nil, err
	}
	mappedImages := []*Image{}
	for _, nbd := range mappedNBDs {
		if nbd.Pool == pool.Name() && nbd.Snapshot == "-" {
			mappedImages = append(mappedImages, pool.getImage(nbd.Name))
			/*
				var mountTime time.Time
				if blkDevStat, err := os.Stat(nbd.Device); err != nil {
					mountTime = blkDevStat.ModTime()
				}
				mappedImages = append(mappedImages, &MappedImage{
					pool.getImage(nbd.Name), nbd.Device, nbd.Pid, mountTime})
			*/
		}
	}
	return mappedImages, nil
}

type devList struct {
	Image    string `json:"image"`
	Snapshot string `json:"snapshot"`
}

// Devices returns all rbd devices including images and snapshots
func (pool *Pool) Devices() ([]Dev, error) {
	devs := []*devList{}
	err := cmdJSON(&devs, poolErrs, pool.cmdArgs("list", "--long")...)
	images := make(map[string]*Image)
	for _, d := range devs {
		if d.Snapshot == "" {
			images[d.Image] = pool.getImage(d.Image)
		}
	}
	retDevs := make([]Dev, 0, len(devs))
	for _, d := range devs {
		image := images[d.Image]
		if d.Snapshot == "" {
			retDevs = append(retDevs, image)
		} else {
			retDevs = append(retDevs, image.getSnapshot(d.Snapshot))
		}
	}
	return retDevs, err
}

var imageErrs = exitCodeToErrMap(map[int]error{2: ErrDoesNotExist})

// GetImage gets an image in the pool
func (pool *Pool) GetImage(name string) (*Image, error) {
	img := pool.getImage(name)
	_, err := img.Info()
	return img, err
}

// ErrAlreadyExists is returned if creating an image that already exists
var ErrAlreadyExists = errors.New("image already exists")

var createErrs = exitCodeToErrMap(map[int]error{
	17: ErrAlreadyExists,
})

// CreateImage creates an image in the pool
func (pool *Pool) CreateImage(name string, size string, args ...string) (*Image, error) {
	args = append([]string{"--image", name, "--size", size}, args...)
	err := cmdRun(createErrs, pool.cmdArgs(args...)...)
	if err != nil && !errors.Is(err, ErrAlreadyExists) {
		return nil, err
	}
	return pool.getImage(name), err
}

// CreateImageWithFileSystem creates and formats an image
func (pool *Pool) CreateImageWithFileSystem(name, size, fileSystem string, args ...string) (*Image, error) {
	img, err := pool.CreateImage(name, size, args...)
	if err != nil {
		return img, err
	}
	blk, err := img.Map()
	if err != nil {
		return img, err
	}
	err = mkfs(blk, fileSystem)
	if err != nil {
		return img, err
	}
	return img, img.Unmap()
}

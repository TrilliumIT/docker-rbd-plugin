package rbd

import (
	"errors"
)

// Pool is an rbd pool
type Pool struct {
	name string
}

func (pool *Pool) Name() string {
	return pool.name
}

func GetPool(name string) *Pool {
	return &Pool{name}
}

// ErrPoolDoesNotExist is returned if the rbd pool does not exist
var ErrPoolDoesNotExist = errors.New("pool does not exist")

func (pool *Pool) cmdArgs(args ...string) []string {
	return append([]string{"--pool", pool.name}, args...)
}

func (pool *Pool) getImage(name string) *Image {
	return getImage(pool, name)
}

var poolErrs = map[int]error{2: ErrPoolDoesNotExist}

// Images returns the rbd images
func (pool *Pool) Images() ([]*Image, error) {
	imgNames := []string{}
	err := cmdJSON(&imgNames, poolErrs, pool.cmdArgs("list")...)
	images := make([]*Image, len(imgNames), len(imgNames))
	for _, n := range imgNames {
		images = append(images, pool.getImage(n))
	}
	return images, err
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
	retDevs := make([]Dev, len(devs), len(devs))
	for _, d := range devs {
		image := images[d.Image]
		if d.Snapshot == "" {
			retDevs = append(retDevs, image)
		} else {
			retDevs = append(retDevs, image.getSnap(d.Snapshot))
		}
	}
	return retDevs, err
}

var imageErrs = map[int]error{2: ErrPoolDoesNotExist}

func (pool *Pool) GetImage(name string) (*Image, error) {
	err := cmdRun(poolErrs, pool.cmdArgs("info", name)...)
	if err != nil {
		return nil, err
	}
	return pool.getImage(name), err
}

var ErrImageAlreadyExists = errors.New("image already exists")

var poolCreateErrs = map[int]error{
	17: ErrImageAlreadyExists,
}

func (pool *Pool) CreateImage(name string, size string, args ...string) (*Image, error) {
	args = append([]string{"--image", name, "--size", size}, args...)
	err := cmdRun(poolCreateErrs, pool.cmdArgs(args...)...)
	if err != nil {
		return nil, err
	}
	return pool.getImage(name), nil
}

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

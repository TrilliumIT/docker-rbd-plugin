package rbddriver

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"

	log "github.com/Sirupsen/logrus"
)

type rbdImage struct {
	name string
	pool string
}

func newRbdImage(pool, name string) (*rbdImage, error) {
	return &rbdImage{pool: pool, name: name}, nil
}

func (ri *rbdImage) isMapped() (bool, error) {
	mapping, err := ri.getMapping()
	if err != nil {
		log.Errorf("Failed to get mapping for image %v.", ri.fullName())
		return false, err
	}

	if mapping == nil {
		return false, nil
	}

	return true, nil
}

func (ri *rbdImage) getDevice() (*rbdDevice, error) {
	mapping, err := ri.getMapping()
	if err != nil {
		log.Errorf("Failed to get mapping for image %v.", ri.fullName())
		return nil, err
	}

	if mapping == nil {
		return nil, fmt.Errorf("Image %v is not mapped, cannot retrieve the device.", ri.fullName())
	}

	return newRbdDevice(mapping["device"])
}

func (ri *rbdImage) isMounted() (bool, error) {
	dev, err := ri.getDevice()
	if err != nil {
		return false, fmt.Errorf("Failed to get device from image %v.", ri.fullName())
	}

	return dev.isMounted()
}

func (ri *rbdImage) getMountPoint() (string, error) {
	dev, err := ri.getDevice()
	if err != nil {
		return "", fmt.Errorf("Failed to get device from image %v.", ri.fullName())
	}

	return dev.getMountPoint()
}

func (ri *rbdImage) fullName() string {
	return ri.pool + "/" + ri.name
}

func (ri *rbdImage) mapDevice() (*rbdDevice, error) {
	b, err := ri.isMapped()
	if err != nil {
		log.Errorf("Failed to detrimine if image %v is already mapped.", ri.fullName())
		return nil, err
	}

	if b {
		dev, err := ri.getDevice()
		if err != nil {
			log.Errorf("Image %v is already mapped and failed to get the device.", ri.fullName())
			return nil, err
		}

		return dev, fmt.Errorf("Image %v is already mapped to %v", ri.fullName(), dev.name)
	}

	out, err := exec.Command("rbd", "map", ri.fullName()).Output()
	if err != nil {
		log.Errorf("Failed to map the image %v.", ri.fullName())
		return nil, err
	}

	return newRbdDevice(strings.TrimSpace(string(out)))
}

func (ri *rbdImage) unmapDevice() error {
	b, err := ri.isMapped()
	if err != nil {
		log.Errorf("Failed to determine if image %v is mapped.", ri.fullName())
		return err
	}

	if !b {
		return fmt.Errorf("Image %v is not currently mapped to a device.", ri.fullName())
	}

	dev, err := ri.getDevice()
	if err != nil {
		log.Errorf("Failed to get the device for image %v.", ri.fullName())
		return err
	}

	b, err = dev.isMounted()
	if err != nil {
		log.Errorf("Failed to determine if image %v at device %v is currently mounted", ri.fullName(), dev.name)
		return err
	}

	if b {
		return fmt.Errorf("Cannot unmap image %v because it is currently mounted to device %v.", ri.fullName(), dev.name)
	}

	err = exec.Command("rbd", "unmap", ri.fullName()).Run()
	if err != nil {
		log.Errorf("Error while trying to unmap the image %v at %v.", ri.fullName(), dev.name)
		return err
	}

	return nil
}

func (ri *rbdImage) create(size string) error {
	b, err := ri.exists()
	if err != nil {
		log.Errorf("Failed to determine if image %v already exists.", ri.fullName())
		return err
	}

	if b {
		return fmt.Errorf("Image %v already exists, cannot create.", ri.fullName())
	}

	err = exec.Command("rbd", "create", ri.fullName(), "--size", size).Run()
	if err != nil {
		log.Errorf("Error while trying to create the image %v.", ri.fullName())
		return err
	}

	return nil
}

func (ri *rbdImage) remove() error {
	b, err := ri.exists()
	if err != nil {
		log.Errorf("Failed to determine if image %v exists.", ri.fullName())
		return err
	}

	if !b {
		return fmt.Errorf("Image %v does not exist, cannot remove.", ri.fullName())
	}

	err = exec.Command("rbd", "remove", ri.fullName()).Run()
	if err != nil {
		log.Errorf("Error while trying to remove image %v.", ri.fullName())
		return err
	}

	return nil
}

func (ri *rbdImage) getMapping() (map[string]string, error) {
	mappings, err := getMappings()
	if err != nil {
		log.Errorf("Failed to retrive the rbd mappings.")
		return nil, err
	}

	for _, rbd := range mappings {
		if rbd["pool"] != ri.pool {
			continue
		}

		if rbd["name"] == ri.name {
			return rbd, nil
		}
	}

	return nil, nil
}

func (ri *rbdImage) exists() (bool, error) {
	images, err := getImages(ri.pool)
	if err != nil {
		log.Errorf("Failed to get the list of rbd images.")
		return false, err
	}

	for _, img := range images {
		if ri.name == img {
			return true, nil
		}
	}

	return false, nil
}

func (ri *rbdImage) lock(id string) error {
	locks, err := ri.getLocks()
	if err != nil {
		log.Errorf(err.Error())
		return fmt.Errorf("Failed to get locks for image %v.", ri.fullName())
	}

	if len(locks) > 0 {
		msg := fmt.Sprintf("Image %v is already locked:", ri.fullName())
		for k := range locks {
			msg = msg + "\n\t" + k
		}
		return fmt.Errorf(msg)
	}

	hn, err := os.Hostname()
	if err != nil {
		log.Errorf(err.Error())
		return fmt.Errorf("Error getting hostname while locking image %v.", ri.fullName())
	}

	lid := hn + ":" + id

	err = exec.Command("rbd", "lock", "add", ri.fullName(), lid).Run()
	if err != nil {
		log.Errorf(err.Error())
		return fmt.Errorf("Failed to acquire lock on image %v with id %v.", ri.fullName(), id)
	}

	return nil
}

func (ri *rbdImage) unlock(id string) error {
	locks, err := ri.getLocks()
	if err != nil {
		log.Errorf(err.Error())
		return fmt.Errorf("Failed to get locks for image %v.", ri.fullName())
	}

	if len(locks) <= 0 {
		return fmt.Errorf("No locks found on image %v.", ri.fullName())
	}

	hn, err := os.Hostname()
	if err != nil {
		log.Errorf(err.Error())
		return fmt.Errorf("Error getting hostname while locking image %v.", ri.fullName())
	}

	lid := hn + ":" + id
	l, ok := locks[lid]

	if !ok {
		return fmt.Errorf("We do not own a lock on image %v with id %v.", ri.fullName(), id)
	}

	err = exec.Command("rbd", "lock", "rm", ri.fullName(), lid, l["locker"]).Run()
	if err != nil {
		log.Errorf(err.Error())
		return fmt.Errorf("Error removing lock from image %v with id %v.", ri.fullName(), id)
	}

	return nil
}

func (ri *rbdImage) getLocks() (map[string]map[string]string, error) {
	bytes, err := exec.Command("rbd", "lock", "list", "--format", "json", ri.fullName()).Output()
	if err != nil {
		log.Errorf(err.Error())
		return nil, fmt.Errorf("Failed to get locks for image %v.", ri.fullName())
	}

	var locks map[string]map[string]string
	err = json.Unmarshal(bytes, &locks)
	if err != nil {
		log.Errorf(err.Error())
		return nil, fmt.Errorf("Failed to unmarshal json: %v", string(bytes))
	}

	return locks, nil
}

func getMappings() (map[string]map[string]string, error) {
	bytes, err := exec.Command("rbd", "showmapped", "--format", "json").Output()
	if err != nil {
		log.Errorf(err.Error())
		return nil, fmt.Errorf("Failed to execute the `rbd showmapped` command.")
	}

	var mappings map[string]map[string]string
	err = json.Unmarshal(bytes, &mappings)
	if err != nil {
		log.Errorf(err.Error())
		return nil, fmt.Errorf("Failed to unmarshal json: %v", string(bytes))
	}

	return mappings, nil
}

func getImages(pool string) ([]string, error) {
	bytes, err := exec.Command("rbd", "list", "--format", "json", pool).Output()
	if err != nil {
		log.Errorf(err.Error())
		return nil, fmt.Errorf("Failed to list images in pool %v.", pool)
	}

	var images []string
	err = json.Unmarshal(bytes, &images)
	if err != nil {
		log.Errorf(err.Error())
		return nil, fmt.Errorf("Failed to unmarshal json: %v", string(bytes))
	}

	return images, nil
}

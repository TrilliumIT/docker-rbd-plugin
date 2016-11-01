package rbddriver

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"strconv"
	"strings"

	log "github.com/Sirupsen/logrus"
)

type mount struct {
	Device     string
	MountPoint string
	FSType     string
	Options    string
	Dump       bool
	FsckOrder  int
}

func GetMounts() (map[string]*mount, error) {
	mounts := make(map[string]*mount)

	mf := os.Getenv("DRP_MOUNTS_FILE")
	if mf == "" {
		mf = "/proc/self/mounts"
	}

	bytes, err := ioutil.ReadFile(mf)
	if err != nil {
		log.Errorf(err.Error())
		return nil, fmt.Errorf("Failed to read mounts file at %v.", mf)
	}

	lines := strings.Split(string(bytes), "\n")
	for _, line := range lines {
		if strings.TrimSpace(line) != "" {
			attrs := strings.Split(line, " ")

			dump, err := strconv.Atoi(attrs[4])
			if err != nil {
				dump = 0
			}
			fo, err := strconv.Atoi(attrs[5])
			if err != nil {
				fo = 0
			}

			mounts[attrs[0]] = &mount{
				Device:     attrs[0],
				MountPoint: attrs[1],
				FSType:     attrs[2],
				Options:    attrs[3],
				Dump:       dump == 1,
				FsckOrder:  fo,
			}
		}
	}

	return mounts, nil
}

func GetMappings() (map[string]map[string]string, error) {
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

func GetImages(pool string) ([]string, error) {
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

func ImageExists(pool, name string) (bool, error) {
	images, err := GetImages(pool)
	if err != nil {
		log.Errorf(err.Error())
		return false, fmt.Errorf("Failed to get image list for pool %v.", pool)
	}

	for _, img := range images {
		if img == name {
			return true, nil
		}
	}

	return false, nil
}

func PoolExists(pool string) (bool, error) {
	err := exec.Command("rbd", "list", pool).Run()
	if err != nil {
		log.Errorf(err.Error())
		return false, fmt.Errorf("Error trying to access pool %v. Does it exist?", pool)
	}

	return true, nil
}

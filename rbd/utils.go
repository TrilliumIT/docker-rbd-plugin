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

//Mount represents a kernel mount
type Mount struct {
	//Device is the mount's device
	Device string
	//MountPoint is the path
	MountPoint string
	//FSType is the filesystem type
	FSType string
	//Options is a string representing the mount's options
	Options string
	//Dump is that first number that no one uses
	Dump bool
	//FsckOrder is the second one
	FsckOrder int
}

//Container represents a container using an rbd device, with it's mountid
type Container struct {
	ID      string
	MountID string
}

//GetMounts returns all kernel mounts
func GetMounts() (map[string]*Mount, error) {
	mounts := make(map[string]*Mount)

	mf := os.Getenv("DRP_MOUNTS_FILE")
	if mf == "" {
		mf = "/proc/self/mounts"
	}

	bytes, err := ioutil.ReadFile(mf)
	if err != nil {
		log.Errorf(err.Error())
		return nil, fmt.Errorf("failed to read mounts file at %v", mf)
	}

	lines := strings.Split(string(bytes), "\n")
	for _, line := range lines {
		if strings.TrimSpace(line) != "" {
			attrs := strings.Split(line, " ")

			var dump int
			dump, err = strconv.Atoi(attrs[4])
			if err != nil {
				dump = 0
			}
			var fo int
			fo, err = strconv.Atoi(attrs[5])
			if err != nil {
				fo = 0
			}

			mounts[attrs[0]] = &Mount{
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

//GetMappings returns all rbd mappings
func GetMappings(pool string) (map[string]map[string]string, error) {
	bytes, err := exec.Command(DrpRbdBinPath, "showmapped", "--format", "json").Output() //nolint: gas
	if err != nil {
		log.Errorf(err.Error())
		return nil, fmt.Errorf("failed to execute the `rbd showmapped` command")
	}

	var mappings map[string]map[string]string
	err = json.Unmarshal(bytes, &mappings)
	if err != nil {
		log.Errorf(err.Error())
		return nil, fmt.Errorf("failed to unmarshal json: %v", string(bytes))
	}

	mymappings := make(map[string]map[string]string)
	for k, v := range mappings {
		if v["pool"] == pool {
			mymappings[k] = v
		}
	}

	return mymappings, nil
}

//GetImages lists all ceph rbds in our pool
func GetImages(pool string) ([]string, error) {
	bytes, err := exec.Command(DrpRbdBinPath, "list", "--format", "json", pool).Output() //nolint: gas
	if err != nil {
		log.Errorf(err.Error())
		return nil, fmt.Errorf("failed to list images in pool %v", pool)
	}

	var images []string
	err = json.Unmarshal(bytes, &images)
	if err != nil {
		log.Errorf(err.Error())
		return nil, fmt.Errorf("failed to unmarshal json: %v", string(bytes))
	}

	return images, nil
}

//ImageExists returns true if image already exists
func ImageExists(image string) (bool, error) {
	pool := strings.Split(image, "/")[0]

	images, err := GetImages(pool)
	if err != nil {
		log.Errorf(err.Error())
		return false, fmt.Errorf("failed to get image list for pool %v", pool)
	}

	for _, img := range images {
		if image == pool+"/"+img {
			return true, nil
		}
	}

	return false, nil
}

//PoolExists returns true if our pool exists
func PoolExists(pool string) (bool, error) {
	err := exec.Command(DrpRbdBinPath, "list", pool).Run() //nolint: gas
	if err != nil {
		log.Errorf(err.Error())
		return false, fmt.Errorf("error trying to access pool %v. Does it exist?", pool)
	}

	return true, nil
}

//GetImagesInUse returns images in use by containers in the container json files
func GetImagesInUse(pool string) (map[string][]*Container, error) {
	images := make(map[string][]*Container)
	dirs, err := ioutil.ReadDir(DrpDockerContainerDir)
	if err != nil {
		log.Error(err.Error())
		return images, fmt.Errorf("error reading container directory %v", DrpDockerContainerDir)
	}

	for _, d := range dirs {
		if !d.IsDir() {
			continue
		}

		var bytes []byte
		bytes, err = ioutil.ReadFile(DrpDockerContainerDir + "/" + d.Name() + "/config.v2.json")
		if err != nil {
			log.WithError(err).WithField("container", d.Name()).Warning("error reading config.v2.json for container")
			continue
		}

		var config map[string]interface{}
		err = json.Unmarshal(bytes, &config)
		if err != nil {
			log.WithError(err).WithField("container", d.Name()).Warning("error during unmarshal of config json")
			continue
		}

		state := config["State"].(map[string]interface{})
		if !state["Running"].(bool) {
			continue
		}

		mps := config["MountPoints"].(map[string]interface{})
		for _, v := range mps {
			m := v.(map[string]interface{})
			if m["Driver"].(string) == "rbd" {
				images[pool+"/"+m["Name"].(string)] = append(images[pool+"/"+m["Name"].(string)], &Container{ID: d.Name(), MountID: m["ID"].(string)})
			}
		}
	}

	return images, nil
}

package rbddriver

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
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
	//Namespace is the namespace the mount is in
	NameSpace string
}

func getMntNS(pidPath string) (string, error) {
	nsPath := filepath.Join(pidPath, "ns", "mnt")
	ns, err := os.Readlink(nsPath)
	if err != nil {
		return "", fmt.Errorf("failed to get ns from %v: %w", nsPath, err)
	}
	return ns, nil
}

//GetMounts returns all kernel mounts in this namespace
func GetMounts(dev string) ([]*Mount, error) {
	ns, err := getMntNS("/proc/self")
	if err != nil {
		return nil, err
	}
	return getMountsFromFile("/proc/self/mounts", dev, ns)
}

//GetOtherNSMounts returns all kernel mounts in other namespaces
func GetOtherNSMounts(dev string) ([]*Mount, error) {
	ns, err := getMntNS("/proc/self")
	if err != nil {
		return nil, err
	}

	// Add self ns to the ns map so it gets skipped preemptively
	namespaces := make(map[string]struct{})
	namespaces[ns] = struct{}{}
	mounts := []*Mount{}

	pidDirs, err := filepath.Glob("/proc/[0-9]*")
	if err != nil {
		return nil, fmt.Errorf("failed to read /proc pids: %w", err)
	}

	var mnts []*Mount
	var file string
	for _, pidDir := range pidDirs {
		ns, err = getMntNS(pidDir)
		if err != nil && errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if _, ok := namespaces[ns]; !ok {
			log.Debugf("pid: %v, ns: %v", pidDir, ns)
			namespaces[ns] = struct{}{}
			file = filepath.Join(pidDir, "mounts")
			mnts, err = getMountsFromFile(file, dev, ns)
			if err != nil && errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err != nil {
				return nil, fmt.Errorf("failed to get mounts from %v: %w", file, err)
			}
			mounts = append(mounts, mnts...)
		}
	}

	return mounts, nil
}

func getMountsFromFile(file, dev, namespace string) ([]*Mount, error) {
	mounts := []*Mount{}
	bytes, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, fmt.Errorf("failed to read mounts file at %v: %w", file, err)
	}

	lines := strings.Split(string(bytes), "\n")
	for _, line := range lines {
		if strings.TrimSpace(line) != "" {
			mount := parseMount(line)
			mount.NameSpace = namespace
			if mount.Device == dev {
				mounts = append(mounts, mount)
			}
		}
	}

	return mounts, nil
}

func parseMount(line string) *Mount {
	attrs := strings.Split(line, " ")

	dump, err := strconv.Atoi(attrs[4])
	if err != nil {
		dump = 0
	}
	var fo int
	fo, err = strconv.Atoi(attrs[5])
	if err != nil {
		fo = 0
	}

	return &Mount{
		Device:     attrs[0],
		MountPoint: attrs[1],
		FSType:     attrs[2],
		Options:    attrs[3],
		Dump:       dump == 1,
		FsckOrder:  fo,
	}
}

type MappedRBD struct {
	Pool   string `json:"pool"`
	Name   string `json:"name"`
	Snap   string `json:"snap"`
	Device string `json:"device"`
}

//GetMappings returns all rbd mappings
func ShowMapped() (map[string]*MappedRBD, error) {
	bytes, err := exec.Command(DrpRbdBinPath, "showmapped", "--format", "json").Output() //nolint: gas
	if err != nil {
		return nil, fmt.Errorf("failed to showmapped: %w", err)
	}

	var maps map[string]*MappedRBD
	err = json.Unmarshal(bytes, &maps)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal %v: %w", string(bytes), err)
	}

	return maps, nil
}

//GetImages lists all ceph rbds in our pool
func ListRBDs(pool string) ([]string, error) {
	bytes, err := exec.Command(DrpRbdBinPath, "list", "--format", "json", pool).Output() //nolint: gas
	if err != nil {
		return nil, fmt.Errorf("failed to list rbds in %v: %w", pool, err)
	}

	var images []string
	err = json.Unmarshal(bytes, &images)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal %v. %w", string(bytes), err)
	}

	return images, nil
}

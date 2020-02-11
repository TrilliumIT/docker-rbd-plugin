package rbd

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

// DrDrpRbdBinPath is the path to the rbd binary
var DrpRbdBinPath string
var fsFreezePath string

func init() {
	var err error
	DrpRbdBinPath, err = exec.LookPath("rbd")
	if err != nil {
		panic(fmt.Errorf("unable to find rbd binary: %w", err))
	}
}

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

//FSFreeze freezes a filesystem
func FSFreeze(mountpoint string) error {
	return fsFreeze(mountpoint, false)
}

//FSUnfreeze freezes a filesystem
func FSUnfreeze(mountpoint string) error {
	return fsFreeze(mountpoint, true)
}

func fsFreeze(mountpoint string, unfreeze bool) error {
	var err error
	if fsFreezePath == "" {
		fsFreezePath, err = exec.LookPath("fsfreeze")
		if err != nil {
			return err
		}
	}
	op := "freeze"
	if unfreeze {
		op = "unfreeze"
	}

	err = exec.Command(fsFreezePath, "--"+op, mountpoint).Run() //nolint: gas
	if err != nil {
		return fmt.Errorf("failed to %v %v: %w", op, mountpoint, err)
	}

	return nil
}

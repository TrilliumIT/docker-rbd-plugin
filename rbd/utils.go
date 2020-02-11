package rbd

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/o1egl/fwencoder"
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
	f, err := os.Open(file)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %v: %w", file, err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
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

//GetImages lists all ceph rbds in our pool
func ListRBDs(pool string) ([]string, error) {
	var images []string
	if err := cmdDecode(jsonDecode(&images), DrpRbdBinPath, "list", "--format", "json", pool); err != nil {
		return nil, fmt.Errorf("failed to list rbds: %w", err)
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

func jsonDecode(v interface{}) func(io.Reader) error {
	return func(r io.Reader) error {
		return json.NewDecoder(r).Decode(v)
	}
}

func colDecode(v interface{}) func(io.Reader) error {
	return func(r io.Reader) error {
		return fwencoder.UnmarshalReader(r, v)
	}
}

func cmdDecode(decode func(io.Reader) error, name string, arg ...string) error {
	cmd := exec.Command(name, arg...)
	stdOut, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("error setting up stdout for cmd %v %v: %w", cmd, arg, err)
	}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("error starting cmd %v %v: %w", cmd, arg, err)
	}
	if err := decode(stdOut); err != nil {
		return fmt.Errorf("error decoding cmd %v %v: %w", cmd, arg, err)
	}
	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("error waiting on cmd %v %v: %w", cmd, arg, err)
	}
	return nil
}

package rbd

import (
	"bufio"
	"encoding/json"
	"errors"
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

var ErrMountedElsewhere = errors.New("device is still mounted in another location")

func isMountedElsewhere(device, mountpoint string) error {
	myNs, err := os.Readlink("/proc/self/ns/mnt")
	if err != nil {
		return fmt.Errorf("error determining my mnt namespace: %w", err)
	}

	myMounts, err := getMountInfoForDevFromFile("/proc/self/mountinfo", device)
	if err != nil {
		return err
	}
	for _, m := range myMounts {
		if m.mountPoint != mountpoint {
			return fmt.Errorf("%v is mounted at %v: %w", device, m.mountPoint, ErrMountedElsewhere)
		}
	}
	if len(myMounts) != 1 {
		return nil
	}
	myMount := myMounts[0]

	namespaces := map[string]struct{}{myNs: struct{}{}}
	proc, err := os.Open("/proc")
	if err != nil {
		return err
	}
	defer proc.Close()
	procDirs, err := proc.Readdir(0)
	if err != nil {
		return err
	}
	for _, procDir := range procDirs {
		if !procDir.IsDir() {
			continue
		}
		pid, err := strconv.Atoi(proc.Name())
		if err != nil {
			continue
		}
		ns, err := os.Readlink("/proc/" + procDir.Name() + "/ns/mnt")
		if err != nil {
			// process could have ended, don't worry about it
			continue
		}
		if _, ok := namespaces[ns]; ok {
			continue
		}
		pidMounts, err := getMountInfoForDevFromFile("/proc/"+procDir.Name()+"/mountinfo", device)
		if err != nil {
			// process could have ended, don't worry about it
			continue
		}
		for _, m := range pidMounts {
			if m.parent.shared == myMount.parent.shared {
				// mounts are in the same peer group
				continue
			}
			if m.parent.master == myMount.parent.shared {
				// mount will recieve events from me when I unmount
				continue
			}
			return fmt.Errorf("%v is mounted at %v in pid %v ns %v with pg shared:%v master:%v: %w", device, m.mountPoint, pid, ns, m.shared, m.master, ErrMountedElsewhere)
		}
		namespaces[ns] = struct{}{}
	}
	return nil
}

func getMountInfoForDevFromFile(mountInfoFile, device string) ([]*mountInfo, error) {
	file, err := os.Open(mountInfoFile)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	mounts := []*mountInfo{}
	otherMounts := make(map[int]*mountInfo)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		m, err := parseMountinfoLine(scanner.Text())
		if err != nil {
			return mounts, err
		}
		if m.source == device {
			mounts = append(mounts, m)
		} else {
			otherMounts[m.id] = m
		}
	}
	for _, m := range mounts {
		parent, ok := otherMounts[m.parentId]
		if !ok {
			return mounts, fmt.Errorf("parent mount not found")
		}
		m.parent = parent
	}
	return mounts, scanner.Err()
}

type mountInfo struct {
	id             int
	parentId       int
	stDev          stDev
	root           string
	mountPoint     string
	mountOptions   []string
	optionalFields []optionalField
	filesystemType string
	source         string
	superOptions   []string
	// extra parsed options
	shared int
	master int
	parent *mountInfo
}

type stDev struct {
	major int
	minor int
}

type optionalField struct {
	tag   string
	value int
}

func parseMountinfoLine(line string) (*mountInfo, error) {
	scanner := bufio.NewScanner(strings.NewReader(line))

	toInt := func(s string) (int, error) {
		i, err := strconv.Atoi(s)
		if err != nil {
			err = fmt.Errorf("unable to convert %v to int: %w", s, err)
		}
		return i, err
	}

	scanFor := func(s string) (string, error) {
		if !scanner.Scan() {
			return "", fmt.Errorf("no more fields when parsing for %v", s)
		}
		return scanner.Text(), nil
	}

	scanIntFor := func(s string) (int, error) {
		s, err := scanFor(s)
		if err != nil {
			return 0, err
		}
		return toInt(s)
	}

	scanOpsFor := func(s string) ([]string, error) {
		s, err := scanFor(s)
		if err != nil {
			return nil, err
		}
		return strings.Split(s, ","), nil
	}

	var field string
	var err error
	m := &mountInfo{}

	m.id, err = scanIntFor("id")
	if err != nil {
		return m, err
	}

	m.parentId, err = scanIntFor("parent id")
	if err != nil {
		return m, err
	}

	if field, err = scanFor("st_dev"); err != nil {
		return m, err
	}
	if devNums := strings.SplitN(field, ":", 2); len(devNums) > 1 {
		m.stDev.major, err = toInt(devNums[0])
		if err != nil {
			return m, err
		}
		m.stDev.minor, err = toInt(devNums[1])
		if err != nil {
			return m, err
		}
	} else {
		return m, fmt.Errorf("only %v of 2 fields in dev number %v", len(devNums), field)
	}

	if m.root, err = scanFor("root"); err != nil {
		return m, err
	}

	if m.mountPoint, err = scanFor("mount point"); err != nil {
		return m, err
	}

	if m.mountOptions, err = scanOpsFor("mount options"); err != nil {
		return m, err
	}

	for field, err = scanFor("optional fields"); field != "-"; field, err = scanFor("optional fields") {
		if err != nil {
			return m, err
		}
		parts := strings.SplitN(field, ":", 2)
		if len(parts) > 1 {
			tag := parts[0]
			if val, err := toInt(parts[1]); err == nil {
				// ignore unknown values by only doing if err is nil
				m.optionalFields = append(m.optionalFields, optionalField{tag, val})
				if tag == "shared" {
					m.shared = val
				}
				if tag == "master" {
					m.master = val
				}
			}
		}
	}

	if m.filesystemType, err = scanFor("filesystem type"); err != nil {
		return m, err
	}
	if m.source, err = scanFor("source"); err != nil {
		return m, err
	}
	if m.superOptions, err = scanOpsFor("super options"); err != nil {
		return m, err
	}

	return m, scanner.Err()
}

package rbd

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
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
		if m.MountPoint != mountpoint {
			return fmt.Errorf("%v is mounted at %v: %w", device, m.MountPoint, ErrMountedElsewhere)
		}
	}
	var myMount *MountInfo
	if len(myMounts) > 0 {
		myMount = myMounts[0]
	}

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
		pid, err := strconv.Atoi(procDir.Name())
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
			if myMount != nil {
				if m.Parent.Shared != 0 && m.Parent.Shared == myMount.Parent.Shared {
					// mounts are in the same peer group
					continue
				}
				if m.Parent.Master != 0 && m.Parent.Master == myMount.Parent.Shared {
					// mount will recieve events from me when I unmount
					continue
				}
			}
			return fmt.Errorf("%v is mounted at %v in pid %v ns %v with pg shared:%v master:%v: %w", device, m.MountPoint, pid, ns, m.Shared, m.Master, ErrMountedElsewhere)
		}
		namespaces[ns] = struct{}{}
	}
	return nil
}

func getMountInfoForDevFromFile(MountInfoFile, device string) ([]*MountInfo, error) {
	file, err := os.Open(MountInfoFile)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	mounts := []*MountInfo{}
	otherMounts := make(map[int]*MountInfo)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		m, err := parseMountinfoLine(scanner.Text())
		if err != nil {
			return mounts, err
		}
		if m.Source == device {
			mounts = append(mounts, m)
		} else {
			otherMounts[m.Id] = m
		}
	}
	for _, m := range mounts {
		parent, ok := otherMounts[m.ParentId]
		if !ok {
			return mounts, fmt.Errorf("parent mount not found")
		}
		m.Parent = parent
	}
	return mounts, scanner.Err()
}

type MountInfo struct {
	Id             int
	ParentId       int
	StDev          stDev
	Root           string
	MountPoint     string
	MountOptions   []string
	OptionalFields []optionalField
	FilesystemType string
	Source         string
	SuperOptions   []string
	// extra parsed options
	Shared int
	Master int
	Parent *MountInfo
}

type stDev struct {
	major int
	minor int
}

type optionalField struct {
	tag   string
	value int
}

func parseMountinfoLine(line string) (*MountInfo, error) {
	scanner := bufio.NewScanner(strings.NewReader(line))
	scanner.Split(bufio.ScanWords)

	toInt := func(s string) (int, error) {
		i, err := strconv.Atoi(s)
		if err != nil {
			err = fmt.Errorf("unable to convert %v to int: %w", s, err)
		}
		return i, err
	}

	scanFor := func(prop string) (string, error) {
		if !scanner.Scan() {
			return "", fmt.Errorf("no more fields when parsing for %v", prop)
		}
		return scanner.Text(), nil
	}

	scanIntFor := func(prop string) (int, error) {
		s, err := scanFor(prop)
		if err != nil {
			return 0, err
		}
		i, err := toInt(s)
		if err != nil {
			err = fmt.Errorf("error parsing for %v: %w", prop, err)
		}
		return i, err
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
	m := &MountInfo{}

	m.Id, err = scanIntFor("id")
	if err != nil {
		return m, err
	}

	m.ParentId, err = scanIntFor("parent id")
	if err != nil {
		return m, err
	}

	if field, err = scanFor("st_dev"); err != nil {
		return m, err
	}
	if devNums := strings.SplitN(field, ":", 2); len(devNums) > 1 {
		m.StDev.major, err = toInt(devNums[0])
		if err != nil {
			return m, err
		}
		m.StDev.minor, err = toInt(devNums[1])
		if err != nil {
			return m, err
		}
	} else {
		return m, fmt.Errorf("only %v of 2 fields in dev number %v", len(devNums), field)
	}

	if m.Root, err = scanFor("root"); err != nil {
		return m, err
	}

	if m.MountPoint, err = scanFor("mount point"); err != nil {
		return m, err
	}

	if m.MountOptions, err = scanOpsFor("mount options"); err != nil {
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
				m.OptionalFields = append(m.OptionalFields, optionalField{tag, val})
				if tag == "shared" {
					m.Shared = val
				}
				if tag == "master" {
					m.Master = val
				}
			}
		}
	}

	if m.FilesystemType, err = scanFor("filesystem type"); err != nil {
		return m, err
	}
	if m.Source, err = scanFor("source"); err != nil {
		return m, err
	}
	if m.SuperOptions, err = scanOpsFor("super options"); err != nil {
		return m, err
	}

	return m, scanner.Err()
}

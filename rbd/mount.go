package rbd

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
)

// MountInfo is information about a mount from /proc/mountinfo
type MountInfo struct {
	ID             int
	ParentID       int
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

// ErrMountedElsewhere is returned when attempting to unmap a device that is still mounted
var ErrMountedElsewhere = errors.New("device is still mounted in another location")

func getMounts(blk string) ([]*MountInfo, error) {
	return getMountInfoForDevFromFile("/proc/self/mountinfo", blk)
}

// Use empty string for blk to get anything mounted here
func isMountedAt(blk, mountPoint string) (bool, error) {
	mounts, err := getMounts(blk)
	if err != nil {
		return false, err
	}
	for _, mount := range mounts {
		if mount.MountPoint == mountPoint {
			return true, nil
		}
	}
	return false, nil
}

func getFs(blk string) (string, error) {
	out, err := exec.Command("deviceid", "-s", "TYPE", "-o", "value", blk).Output()
	if err != nil {
		return "", fmt.Errorf("error determining filesystem on %v: %w", blk, err)
	}
	return strings.TrimSpace(string(out)), nil
}

func mount(blk, mountPoint, fs string, flags uintptr, data string) error {
	if mounted, err := isMountedAt(blk, mountPoint); err != nil || mounted {
		return err
	}

	if fs == "" {
		var err error
		fs, err = getFs(blk)
		if err != nil {
			return err
		}
	}

	if err := os.MkdirAll(mountPoint, 0755); err != nil {
		return fmt.Errorf("error creating directory: %v: %w", mountPoint, err)
	}

	if err := syscall.Mount(blk, mountPoint, fs, flags, data); err != nil {
		return fmt.Errorf("error mounting %v to %v as %v: %w", blk, mountPoint, fs, err)
	}

	return nil
}

func unmount(blk, mountPoint string) error {
	mounted, err := isMountedAt(blk, mountPoint)
	if err != nil || !mounted {
		return err
	}
	if err = syscall.Unmount(mountPoint, 0); err != nil {
		err = fmt.Errorf("error unmounting %v from %v: %w", blk, mountPoint, err)
	}
	return err
}

func mkfs(blk, fs string) error {
	return exec.Command("mkfs."+fs, blk).Run()
}

func isMountedElsewhere(blk, mountpoint string) error {
	myNs, err := os.Readlink("/proc/self/ns/mnt")
	if err != nil {
		return fmt.Errorf("error determining my mnt namespace: %w", err)
	}

	myMounts, err := getMounts(blk)
	if err != nil {
		return err
	}
	for _, m := range myMounts {
		if m.MountPoint != mountpoint {
			return fmt.Errorf("%v is mounted at %v: %w", blk, m.MountPoint, ErrMountedElsewhere)
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
		pidMounts, err := getMountInfoForDevFromFile("/proc/"+procDir.Name()+"/mountinfo", blk)
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
			return fmt.Errorf("%v is mounted at %v in pid %v ns %v with pg shared:%v master:%v: %w", blk, m.MountPoint, pid, ns, m.Shared, m.Master, ErrMountedElsewhere)
		}
		namespaces[ns] = struct{}{}
	}
	return nil
}

func getMountInfoForDevFromFile(MountInfoFile, blk string) ([]*MountInfo, error) {
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
		if m.Source == blk || blk == "" {
			mounts = append(mounts, m)
		} else {
			otherMounts[m.ID] = m
		}
	}
	for _, m := range mounts {
		parent, ok := otherMounts[m.ParentID]
		if !ok {
			return mounts, fmt.Errorf("parent mount not found")
		}
		m.Parent = parent
	}
	return mounts, scanner.Err()
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

	m.ID, err = scanIntFor("id")
	if err != nil {
		return m, err
	}

	m.ParentID, err = scanIntFor("parent id")
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

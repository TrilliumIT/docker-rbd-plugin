package rbd

import (
	"errors"
	"fmt"
	"os/exec"
)

type Snap struct {
	dev
	image *Image
}

func getSnap(img *Image, name string) *Snap {
	return &Snap{dev{name: name}, img}
}

func (snap *Snap) FullName() string {
	return snap.image.FullName() + "@" + snap.Name()
}

func (snap *Snap) Pool() *Pool {
	return snap.image.Pool()
}

type Snapshot struct {
	*RBD
	Snapshot        string
	Objects         int           `json:"objects"`
	Order           int           `json:"order"`
	ObjectSize      int           `json:"object_size"`
	BlockNamePrefix string        `json:"block_name_prefix"`
	Format          int           `json:"format"`
	Flags           []interface{} `json:"flags"`
	CreateTimestamp string        `json:"create_timestamp"`
	Protected       string        `json:"protected"`
}

func (sn *Snapshot) SnapName() string {
	return sn.Pool + "/" + sn.Name + "@" + sn.Snapshot
}

func (rbd *RBD) toSnapName(name string) string {
	return rbd.RBDName() + "@" + name
}

// Snapshot snapshots an rbd
func (rbd *RBD) Snapshot(name string) (*Snapshot, error) {
	snapName := rbd.toSnapName(name)

	err := exec.Command(DrpRbdBinPath, "snap", "create", snapName).Run() //nolint: gas
	if err != nil {
		return nil, fmt.Errorf("error creating snapshot %v: %w", snapName, err)
	}

	return rbd.getSnapshot(name)
}

// GetSnapshot snapshots an rbd
func (rbd *RBD) GetSnapshot(name string) (*Snapshot, error) {
	return rbd.getSnapshot(name)
}

// ErrNoRBD is returned when an rbd does not exist
var ErrNoSnap = errors.New("snapshot does not exist")

func (rbd *RBD) getSnapshot(name string) (*Snapshot, error) {
	snapName := rbd.toSnapName(name)

	snap := &Snapshot{RBD: &RBD{Pool: rbd.Pool}, Snapshot: name}
	if err := cmdDecode(jsonDecode(snap), DrpRbdBinPath, "info", "--format", "json", snapName); err != nil {
		return nil, fmt.Errorf("error getting snapshot: %w", err)
	}
	return snap, nil
}

func (sn *Snapshot) Remove() error {
	err := exec.Command(DrpRbdBinPath, "snap", "remove", "--no-progress", sn.SnapName()).Run() //nolint: gas
	if err != nil {
		return fmt.Errorf("error removing %v: %w", sn.SnapName(), err)
	}
	return nil
}

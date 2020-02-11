package rbd

import (
	"errors"
	"fmt"
	"os/exec"
	"sync"
)

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
	mutex := getMutex(snapName)
	mutex.Lock()

	err := exec.Command(DrpRbdBinPath, "snap", "create", snapName).Run() //nolint: gas
	if err != nil {
		return nil, fmt.Errorf("error creating snapshot %v: %w", snapName, err)
	}

	return rbd.getSnapshot(name, mutex)
}

// GetSnapshot snapshots an rbd
func (rbd *RBD) GetSnapshot(name string) (*Snapshot, error) {
	mutex := getMutex(rbd.toSnapName(name))
	mutex.Lock()
	return rbd.getSnapshot(name, mutex)
}

// ErrNoRBD is returned when an rbd does not exist
var ErrNoSnap = errors.New("snapshot does not exist")

func (rbd *RBD) getSnapshot(name string, mutex *sync.Mutex) (*Snapshot, error) {
	snapName := rbd.toSnapName(name)

	snap := &Snapshot{RBD: &RBD{Pool: rbd.Pool, mutex: mutex}, Snapshot: name}
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

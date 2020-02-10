package rbddriver

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

const (
	//DrpLockTagSuffix is the suffix for the lock tag
	DrpLockTagSuffix = "_docker-rbd-plugin"
	//DrpNoUsrMaxCnt is the maximum number of lock refreshes with no users
	DrpNoUsrMaxCnt = 3
)

//RbdLock represents an actively refreshed ceph lock on a ceph rbd
type RbdLock struct {
	hostname string
	img      *RbdImage
	ticker   *time.Ticker
	open     chan struct{}
	noUsrCnt int
}

//AcquireLock locks the rbd, and starts a goroutine to maintain the lock
func AcquireLock(img *RbdImage, expireSeconds int) (*RbdLock, error) {
	b, err := img.IsLocked()
	if err != nil {
		log.Errorf(err.Error())
		return nil, fmt.Errorf("error trying to determine if image %v is currently locked", img.image)
	}

	if b {
		return nil, fmt.Errorf("image already has a valid lock held")
	}

	return InheritLock(img, expireSeconds)
}

//InheritLock inherits an existing rbd lock and starts a goroutine to maintain it
func InheritLock(img *RbdImage, expireSeconds int) (*RbdLock, error) {
	expiresIn := time.Duration(expireSeconds) * time.Second
	refresh := time.Duration(int(float32(expireSeconds)*(DrpRefreshPercent/100.0))) * time.Second

	hn, err := os.Hostname()
	if err != nil {
		log.Errorf(err.Error())
		return nil, fmt.Errorf("error getting hostname while acquiring a lock for image %v", img.image)
	}

	var ti *time.Ticker
	if expireSeconds > 0 {
		ti = time.NewTicker(refresh)
	}

	rl := &RbdLock{hostname: hn, img: img, ticker: ti, open: make(chan struct{}), noUsrCnt: 0}
	err = rl.img.reapLocks()
	if err != nil {
		log.Errorf(err.Error())
		return rl, fmt.Errorf("error while reaping old locks before adding our initial lock")
	}
	go rl.refreshLoop(expiresIn)

	return rl, nil
}

func (rl *RbdLock) addLock(expires time.Time) (string, error) {
	lid := rl.hostname + "," + expires.Format(time.RFC3339Nano)
	tag := rl.hostname + DrpLockTagSuffix

	err := exec.Command(DrpRbdBinPath, "lock", "add", "--shared", tag, rl.img.image, lid).Run() //nolint: gas
	if err != nil {
		log.Errorf(err.Error())
		return "", fmt.Errorf("failed to add lock to image %v", rl.img.image)
	}

	return lid, nil
}

func (rl *RbdLock) refreshLock(expiresIn time.Duration) error {
	err := rl.img.users.reconcile(rl.img.image)
	if err != nil {
		log.WithError(err).WithField("image", rl.img.image).Error("failed to reconcile image users")
	}

	if rl.noUsrCnt > 0 && rl.img.users.len() > 0 {
		rl.noUsrCnt = 0
	}

	if rl.img.users.len() == 0 {
		if rl.noUsrCnt > DrpNoUsrMaxCnt {
			log.Warnf("No users using the image %v. Releasing lock", rl.img.image)
			return rl.img.Unmount("")
		}
		rl.noUsrCnt++
	}

	exp := time.Now().Add(expiresIn)

	if expiresIn.Seconds() == 0 {
		exp = DrpEndOfTime
	}

	lid, err := rl.addLock(exp)
	if err != nil {
		log.WithError(err).WithField("image", rl.img.image).Error("error adding lock to image")
	}

	locks, err := rl.img.GetAllLocks()
	if err != nil {
		log.Errorf(err.Error())
		return fmt.Errorf("error retrieving locks for image %v", rl.img.image)
	}

	for k, v := range locks {
		if lid == k {
			continue
		}

		lock := strings.Split(k, ",")
		if rl.hostname != lock[0] {
			log.WithField("hostname", lock[0]).Warning("encountered a lock held by a different host while updating our lock. Image should not be locked by multiple hosts")
			continue
		}

		err = rl.img.removeLock(k, v["locker"])
		if err != nil {
			log.WithError(err).Error("encountered error removing stale lock")
		}
	}

	return nil
}

func (rl *RbdLock) release() error {
	select {
	case <-rl.open:
	default:
		close(rl.open)
	}

	locks, err := rl.img.GetAllLocks()
	if err != nil {
		log.Errorf(err.Error())
		return fmt.Errorf("error while getting locks for image %v", rl.img.image)
	}

	for k, v := range locks {
		lock := strings.Split(k, ",")
		if rl.hostname != lock[0] {
			log.Errorf("encountered a lock held by a different host while releasing our lock. Image should not be locked by multiple hosts")
			continue
		}

		err = rl.img.removeLock(k, v["locker"])
		if err != nil {
			log.WithError(err).Error("encountered error removing lock")
		}
	}

	return nil
}

func (rl *RbdLock) refreshLoop(expiresIn time.Duration) {
	err := rl.refreshLock(expiresIn)
	if err != nil {
		log.WithError(err).WithField("image", rl.img.image).Error("error while creating an initial lock on image")
		return
	}

	if expiresIn.Seconds() == 0 {
		log.Info("bypassing refresh loop for fixed lock")
		return
	}

	for {
		select {
		case <-rl.open:
			log.WithField("image", rl.img.image).Debug("lock channel shut, closing refresh loop")
			return
		case <-rl.ticker.C:
			err = rl.refreshLock(expiresIn)
			if err != nil {
				log.WithError(err).WithField("image", rl.img.image).Error("error refreshing lock")
				continue
			}
		}
	}
}

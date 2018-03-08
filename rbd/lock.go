package rbddriver

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
)

const (
	DRP_LOCK_TAG_SUFFIX = "_docker-rbd-plugin"
)

type rbdLock struct {
	hostname string
	img      *RbdImage
	ticker   *time.Ticker
	open     chan struct{}
}

func AcquireLock(img *RbdImage, expireSeconds int) (*rbdLock, error) {
	b, err := img.IsLocked()
	if err != nil {
		log.Errorf(err.Error())
		return nil, fmt.Errorf("Error trying to determine if image %v is currently locked.", img.image)
	}

	if b {
		return nil, fmt.Errorf("Image already has a valid lock held.")
	}

	return InheritLock(img, expireSeconds)
}

func InheritLock(img *RbdImage, expireSeconds int) (*rbdLock, error) {
	expiresIn := time.Duration(expireSeconds) * time.Second
	refresh := time.Duration(int(float32(expireSeconds)*(DrpRefreshPercent/100.0))) * time.Second

	hn, err := os.Hostname()
	if err != nil {
		log.Errorf(err.Error())
		return nil, fmt.Errorf("Error getting hostname while acquiring a lock for image %v.", img.image)
	}

	var ti *time.Ticker
	if expireSeconds > 0 {
		ti = time.NewTicker(refresh)
	}

	rl := &rbdLock{hostname: hn, img: img, ticker: ti, open: make(chan struct{})}
	err = rl.img.reapLocks()
	if err != nil {
		log.Errorf(err.Error())
		return rl, fmt.Errorf("Error while reaping old locks before adding our initial lock.")
	}
	go rl.refreshLoop(expiresIn)

	return rl, nil
}

func (rl *rbdLock) addLock(expires time.Time) (string, error) {
	lid := rl.hostname + "," + expires.Format(time.RFC3339Nano)
	tag := rl.hostname + DRP_LOCK_TAG_SUFFIX

	err := exec.Command("rbd", "lock", "add", "--shared", tag, rl.img.image, lid).Run()
	if err != nil {
		log.Errorf(err.Error())
		return "", fmt.Errorf("Failed to add lock to image %v.", rl.img.image)
	}

	return lid, nil
}

func (rl *rbdLock) refreshLock(expiresIn time.Duration) error {
	exp := time.Now().Add(expiresIn)

	if expiresIn.Seconds() == 0 {
		exp = DrpEndOfTime
	}

	lid, err := rl.addLock(exp)
	if err != nil {
		log.WithError(err).WithField("image", rl.img.image).Error("Error adding lock to image.")
	}

	locks, err := rl.img.GetAllLocks()
	if err != nil {
		log.Errorf(err.Error())
		return fmt.Errorf("Error retrieving locks for image %v.", rl.img.image)
	}

	for k, v := range locks {
		if lid == k {
			continue
		}

		lock := strings.Split(k, ",")
		if rl.hostname != lock[0] {
			log.WithField("hostname", lock[0]).Warning("Encounted a lock held by a different host while updating our lock. Image should not be locked by multiple hosts.")
			continue
		}

		rl.img.removeLock(k, v["locker"])
	}

	return nil
}

func (rl *rbdLock) release() error {
	select {
	case _ = <-rl.open:
	default:
		close(rl.open)
	}

	locks, err := rl.img.GetAllLocks()
	if err != nil {
		log.Errorf(err.Error())
		return fmt.Errorf("Error while getting locks for image %v.", rl.img.image)
	}

	for k, v := range locks {
		lock := strings.Split(k, ",")
		if rl.hostname != lock[0] {
			log.Errorf("Encounted a lock held by a different host while releasing our lock. Image should not be locked by multiple hosts.")
			continue
		}

		rl.img.removeLock(k, v["locker"])
	}

	return nil
}

func (rl *rbdLock) refreshLoop(expiresIn time.Duration) {
	err := rl.refreshLock(expiresIn)
	if err != nil {
		log.WithError(err).WithField("image", rl.img.image).Error("Error while creating a initial lock on image.")
		return
	}

	if expiresIn.Seconds() == 0 {
		log.Info("Bypassing refresh loop for fixed lock.")
		return
	}

	for {
		select {
		case <-rl.open:
			log.WithField("image", rl.img.image).Debug("Lock channel shut, closing refresh loop")
			return
		case <-rl.ticker.C:
			err := rl.refreshLock(expiresIn)
			if err != nil {
				log.WithError(err).WithField("image", rl.img.image).Error("Error refreshing lock.")
				continue
			}
		}
	}
}

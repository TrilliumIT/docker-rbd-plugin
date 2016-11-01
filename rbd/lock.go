package rbddriver

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
)

const (
	DRP_REFRESH_PERCENT = 50
)

type rbdLock struct {
	hostname string
	image    string
	tag      string
	ticker   *time.Ticker
	open     chan struct{}
}

func AcquireLock(image, tag string, expireSeconds int) (*rbdLock, error) {
	expiresIn := time.Duration(expireSeconds) * time.Second
	refresh := time.Duration(int(float32(expireSeconds)*(DRP_REFRESH_PERCENT/100.0))) * time.Second

	b, err := IsImageLocked(image)
	if err != nil {
		log.Errorf(err.Error())
		return nil, fmt.Errorf("Error trying to determine if image %v is currently locked.", image)
	}

	if b {
		return nil, fmt.Errorf("Image already has a valid lock held.")
	}

	hn, err := os.Hostname()
	if err != nil {
		log.Errorf(err.Error())
		return nil, fmt.Errorf("Error getting hostname while acquiring a lock for image %v.", image)
	}

	var ti *time.Ticker
	if expireSeconds > 0 {
		ti = time.NewTicker(refresh)
	}

	rl := &rbdLock{hostname: hn, image: image, tag: tag, ticker: ti, open: make(chan struct{})}
	go rl.refreshLoop(expiresIn)

	return rl, nil
}

func AcquireFixedLock(image, tag string) (*rbdLock, error) {
	return AcquireLock(image, tag, 0)
}

func (rl *rbdLock) addLock(expires time.Time) (string, error) {
	lid := rl.hostname + "," + expires.Format(time.RFC3339Nano)

	err := exec.Command("rbd", "lock", "add", "--shared", rl.tag, rl.image, lid).Run()
	if err != nil {
		log.Errorf(err.Error())
		return "", fmt.Errorf("Failed to acquire lock on image %v.", rl.image)
	}

	return lid, nil
}

func (rl *rbdLock) refreshLock(expiresIn time.Duration) error {
	exp := time.Now().Add(expiresIn)

	if expiresIn.Seconds() == 0 {
		exp = time.Unix(1<<63-62135596801, 999999999)
	}

	lid, err := rl.addLock(exp)
	if err != nil {
		log.WithError(err).WithField("image", rl.image).Error("Error adding lock to image.")
	}

	locks, err := getImageLocks(rl.image)
	if err != nil {
		log.Errorf(err.Error())
		return fmt.Errorf("Error retrieving locks for image %v.", rl.image)
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

		rl.removeLock(k, v["locker"])
	}

	return nil
}

func (rl *rbdLock) removeLock(id, locker string) error {
	err := exec.Command("rbd", "lock", "rm", rl.image, id, locker).Run()
	if err != nil {
		log.Errorf(err.Error())
		return fmt.Errorf("Error removing lock from image %v with id %v.", rl.image, id)
	}

	return nil
}

func (rl *rbdLock) release() error {
	select {
	case _ = <-rl.open:
	default:
		close(rl.open)
	}

	locks, err := getImageLocks(rl.image)
	if err != nil {
		log.Errorf(err.Error())
		return fmt.Errorf("Error while getting locks for image %v.", rl.image)
	}

	for k, v := range locks {
		lock := strings.Split(k, ",")
		if rl.hostname != lock[0] {
			log.Errorf("Encounted a lock held by a different host while releasing our lock. Image should not be locked by multiple hosts.")
			continue
		}

		rl.removeLock(k, v["locker"])
	}

	return nil
}

func (rl *rbdLock) refreshLoop(expiresIn time.Duration) {
	err := rl.refreshLock(expiresIn)
	if err != nil {
		log.WithError(err).WithField("image", rl.image).Error("Error while creating a initial lock on image.")
		return
	}

	if expiresIn.Seconds() == 0 {
		log.WithField("lock tag", rl.tag).Info("Bypassing refresh loop for fixed lock.")
		return
	}

	for {
		select {
		case <-rl.open:
			log.Debug("lock channel shut, closing refresh loop")
			return
		case <-rl.ticker.C:
			err := rl.refreshLock(expiresIn)
			if err != nil {
				log.WithError(err).WithField("image", rl.image).Error("Error refreshing lock.")
				continue
			}
		}
	}
}

func (rl *rbdLock) reapLocks() error {
	locks, err := getImageLocks(rl.image)
	if err != nil {
		log.Errorf(err.Error())
		return fmt.Errorf("Error while getting locks for image %v.", rl.image)
	}

	for k, v := range locks {
		lock := strings.Split(k, ",")

		t, err := time.Parse(time.RFC3339Nano, lock[1])
		if err != nil {
			log.WithError(err).WithField("lock id", k).Warning("Error while parsing time from lock id.")
			continue
		}

		if time.Now().After(t) {
			log.WithFields(log.Fields{
				"lock id": k,
				"locker":  v["locker"],
				"address": v["address"],
			}).Info("Reaping expired lock.")
			rl.removeLock(k, v["locker"])
		}
	}

	return nil
}

func getImageLocks(image string) (map[string]map[string]string, error) {
	bytes, err := exec.Command("rbd", "lock", "list", "--format", "json", image).Output()
	if err != nil {
		log.Errorf(err.Error())
		return nil, fmt.Errorf("Failed to get locks for image %v.", image)
	}

	var locks map[string]map[string]string
	err = json.Unmarshal(bytes, &locks)
	if err != nil {
		log.Errorf(err.Error())
		return nil, fmt.Errorf("Failed to unmarshal json: %v", string(bytes))
	}

	return locks, nil
}

func IsImageLocked(image string) (bool, error) {
	locks, err := getImageLocks(image)
	if err != nil {
		log.Errorf(err.Error())
		return false, fmt.Errorf("Error while getting locks for image %v.", image)
	}

	for k := range locks {
		lock := strings.Split(k, ",")

		t, err := time.Parse(time.RFC3339Nano, lock[1])
		if err != nil {
			log.Warningf(err.Error())
			log.Warningf("Error while parsing time from lock id %v.", k)
			continue
		}

		if time.Now().Before(t) {
			return true, nil
		}
	}

	return false, nil
}

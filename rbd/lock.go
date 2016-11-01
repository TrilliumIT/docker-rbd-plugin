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

type rbdLock struct {
	hostname string
	image    string
	tag      string
	timer    *time.Timer
	open     chan struct{}
}

func AcquireLock(image, tag string, refreshInterval int) (*rbdLock, error) {
	//TODO: first make sure there are no existing locks!
	hn, err := os.Hostname()
	if err != nil {
		log.Errorf(err.Error())
		return nil, fmt.Errorf("Error getting hostname while acquiring a lock for image %v.", image)
	}

	exp := time.Unix(1<<63-62135596801, 999999999)
	if refreshInterval > 0 {
		exp = time.Now().Add(time.Duration(refreshInterval) * time.Second)
	}

	rl := &rbdLock{hostname: hn, image: image, tag: tag}

	err = rl.addLock(exp)
	if err != nil {
		log.Errorf(err.Error())
		return nil, fmt.Errorf("Error creating initial lock.")
	}

	rl.open = make(chan struct{})

	if refreshInterval > 0 {
		rl.timer = time.NewTimer(time.Duration(refreshInterval) * time.Second)
		go rl.refreshLoop(refreshInterval)
	}

	return rl, nil
}

func AcquireFixedLock(image, tag string) (*rbdLock, error) {
	return AcquireLock(image, tag, 0)
}

func (rl *rbdLock) addLock(expires time.Time) error {
	lid := rl.hostname + "," + expires.Format(time.RFC3339Nano)

	err := exec.Command("rbd", "lock", "add", "--shared", rl.tag, rl.image, lid).Run()
	if err != nil {
		log.Errorf(err.Error())
		return fmt.Errorf("Failed to acquire lock on image %v.", rl.image)
	}
	return nil
}

func (rl *rbdLock) refreshLock() error {
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
	close(rl.open)

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

func (rl *rbdLock) refreshLoop(refreshInterval int) {
	for {
		select {
		case <-rl.open:
			log.Debug("lock channel shut, closing refresh loop")
			return
		case <-rl.timer.C:
			log.Debug("updating lock")

			rl.timer = time.NewTimer(time.Duration(refreshInterval) * time.Second)
			rl.addLock(time.Now().Add(time.Duration(refreshInterval) * time.Second))
			locks, err := getImageLocks(rl.image)
			if err != nil {
				log.Errorf(err.Error())
				log.Errorf("Error retrieving locks for image %v.", rl.image)
				continue
			}

			for k, v := range locks {
				lock := strings.Split(k, ",")
				if rl.hostname != lock[0] {
					log.Errorf("Encounted a lock held by a different host while updating our lock. Image should not be locked by multiple hosts.")
					continue
				}

				t, err := time.Parse(time.RFC3339Nano, lock[1])
				if err != nil {
					log.Errorf(err.Error())
					log.Errorf("Error while parsing time from lock id %v.", k)
					continue
				}

				if time.Now().After(t) {
					rl.removeLock(k, v["locker"])
				}
			}
		}
	}
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

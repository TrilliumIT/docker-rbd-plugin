package rbddriver

import (
	"sync"
)

type rbdUsers struct {
	users map[string]struct{}
	lock  sync.RWMutex
}

func (u *rbdUsers) add(id string) {
	u.lock.Lock()
	defer u.lock.Unlock()
	u.users[id] = struct{}{}
}

func (u *rbdUsers) remove(id string) {
	u.lock.Lock()
	defer u.lock.Unlock()
	delete(u.users, id)
}

func (u *rbdUsers) clear() {
	u.lock.Lock()
	defer u.lock.Unlock()
	u.users = make(map[string]struct{})
}

func (u *rbdUsers) len() int {
	u.lock.RLock()
	defer u.lock.RUnlock()
	return len(u.users)
}

func (u *rbdUsers) reconcile(image string) error {
	conts, err := GetContainersUsingImage(image)
	if err != nil {
		return err
	}

	u.clear()

	for _, c := range conts {
		u.add(c.MountID)
	}

	return nil
}

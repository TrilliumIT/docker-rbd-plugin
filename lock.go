package main

import (
	"sync"

	"github.com/TrilliumIT/docker-rbd-plugin/rbd"
)

var mutexMapMutex = &sync.RWMutex{}
var mutexMap = make(map[string]*sync.Mutex)

func getMutex(name string) *sync.Mutex {
	mutexMapMutex.RLock()
	mutex := mutexMap[name]
	if mutex == nil {
		mutexMapMutex.RUnlock()
		mutexMapMutex.Lock()
		mutex = mutexMap[name]
		if mutex == nil {
			mutex = &sync.Mutex{}
			mutexMap[name] = mutex
		}
		mutexMapMutex.Unlock()
	} else {
		mutexMapMutex.RUnlock()
	}
	return mutex
}

func lock(s string) {
	getMutex(s).Lock()
}

func unlock(s string) {
	getMutex(s).Unlock()
}

func lockDev(dev rbd.Dev) {
	lock(dev.FullName())
}

func unlockDev(dev rbd.Dev) {
	getMutex(dev.FullName()).Unlock()
}

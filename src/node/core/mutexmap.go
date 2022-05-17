package core

import (
	"sync"
)

type LockMap struct {
	mainLock sync.Mutex
	locks    map[interface{}]*entry
}

type entry struct {
	m         *LockMap
	entryLock sync.Mutex
	count     int
	key       interface{}
}

type Unlocker interface {
	Unlock()
}

func NewLockMap() *LockMap {
	return &LockMap{locks: make(map[interface{}]*entry)}
}

func (m *LockMap) Lock(key interface{}) {
	m.mainLock.Lock()
	e, ok := m.locks[key]
	if !ok {
		e = &entry{m: m, key: key}
		m.locks[key] = e
	}
	e.count++
	m.mainLock.Unlock()
	e.entryLock.Lock()
}

func (m *LockMap) Unlock(key interface{}) {
	m.mainLock.Lock()
	entry, exists := m.locks[key]
	if !exists {
		m.mainLock.Unlock()
		return
	}
	entry.count--
	if entry.count == 0 {
		delete(m.locks, key)
	}
	m.mainLock.Unlock()
	entry.entryLock.Unlock()
}

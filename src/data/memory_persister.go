package data

import "sync"

type MemoryPersister struct {
	mu       sync.Mutex
	state    []byte
	snapshot []byte
}

func MakeMemoryPersister() *MemoryPersister {
	return &MemoryPersister{}
}

func (ps *MemoryPersister) Copy() *MemoryPersister {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	np := MakeMemoryPersister()
	np.state = ps.state
	np.snapshot = ps.snapshot
	return np
}

func (ps *MemoryPersister) SaveState(data []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.state = data
}

func (ps *MemoryPersister) ReadState() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ps.state
}

func (ps *MemoryPersister) StateSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.state)
}

func (ps *MemoryPersister) SaveSnapshot(snapshot []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.snapshot = snapshot
}

func (ps *MemoryPersister) ReadSnapshot() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ps.snapshot
}

func (ps *MemoryPersister) SnapshotSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.snapshot)
}

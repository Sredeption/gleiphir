package data

type Persister interface {
	SaveState(data []byte)
	ReadState() []byte
	StateSize() int
	SaveSnapshot(snapshot []byte)
	ReadSnapshot() []byte
	SnapshotSize() int
}

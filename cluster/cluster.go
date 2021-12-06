package cluster

type Lock interface {
	Lock()
	Unlock()
}

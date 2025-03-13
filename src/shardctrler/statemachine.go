package shardctrler

// TODO

type MemoryKVStateMachine struct {
}

func NewMemoryKVMachine() *MemoryKVStateMachine {
	return &MemoryKVStateMachine{}
}

func (mkv *MemoryKVStateMachine) Join(servers map[int][]string) Err {
	return OK
}

func (mkv *MemoryKVStateMachine) Leave(gids []int) Err {
	return OK
}

func (mkv *MemoryKVStateMachine) Move(shard, gid int) Err {
	return OK
}

func (mkv *MemoryKVStateMachine) Query(num int) (Config, Err) {
	return Config{}, OK
}

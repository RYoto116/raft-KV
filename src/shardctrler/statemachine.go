package shardctrler

import "sort"

type CtrlerStateMachine struct {
	Configs []Config
}

func NewCtrlerStateMachine() *CtrlerStateMachine {
	cf := &CtrlerStateMachine{
		Configs: make([]Config, 1), // 设置默认config
	}
	cf.Configs[0] = DefaultConfig()
	return cf
}

func (csm *CtrlerStateMachine) Join(groups map[int][]string) Err {
	// 构建新配置
	lastConfig := csm.Configs[len(csm.Configs)-1]
	newConfig := Config{
		Num:    len(csm.Configs),
		Shards: lastConfig.Shards,
		Groups: copyGroups(lastConfig.Groups),
	}

	// 将新group加入集群
	for gid, servers := range groups {
		if _, ok := newConfig.Groups[gid]; !ok {
			newConfig.Groups[gid] = make([]string, len(servers))
			copy(newConfig.Groups[gid], servers)
		}
	}

	// 构造gid --> shardID 映射关系
	gidToShards := make(map[int][]int)
	for gid := range newConfig.Groups {
		gidToShards[gid] = make([]int, 0) // 防止空指针
	}

	for sharID, gid := range newConfig.Shards {
		gidToShards[gid] = append(gidToShards[gid], sharID)
	}

	// 进行shard迁移，需要找出最多shard/最少shard的groupID
	for {
		maxGid, minGid := gidWithMaxShards(gidToShards), gidWithMinShards(gidToShards)
		if maxGid != 0 && len(gidToShards[maxGid])-len(gidToShards[minGid]) <= 1 {
			break
		}

		// 最少shard的group增加一个shard
		gidToShards[minGid] = append(gidToShards[minGid], gidToShards[maxGid][0])
		// 最多shard的group减少对应的shard
		gidToShards[maxGid] = gidToShards[maxGid][1:]
	}

	// 将新的映射关系存储到newConfig中
	var newShards [NShards]int
	for gid, shards := range gidToShards {
		for _, shard := range shards {
			newShards[shard] = gid
		}
	}
	newConfig.Shards = newShards
	csm.Configs = append(csm.Configs, newConfig)

	return OK
}

func (csm *CtrlerStateMachine) Leave(gids []int) Err {
	lastConfig := csm.Configs[len(csm.Configs)-1]
	newConfig := Config{
		Num:    len(csm.Configs),
		Shards: lastConfig.Shards,
		Groups: copyGroups(lastConfig.Groups),
	}

	gidToShards := make(map[int][]int)
	for gid := range newConfig.Groups {
		gidToShards[gid] = make([]int, 0) // 防止空指针
	}
	for sharID, gid := range newConfig.Shards {
		gidToShards[gid] = append(gidToShards[gid], sharID)
	}

	// 需要重新分配的unassignedShards
	var unassignedShards []int
	for _, gid := range gids {
		// 如果gid在newConfig的groups中，删除
		delete(newConfig.Groups, gid)

		// 取出被删除group所负责的shard
		if shards, ok := gidToShards[gid]; ok {
			unassignedShards = append(unassignedShards, shards...)
			delete(gidToShards, gid)
		}
	}

	// shard重新分配
	for _, shard := range unassignedShards {
		// 重要！！！groups有可能全部删除，此时-1越界
		if len(newConfig.Groups) != 0 {
			minGid := gidWithMinShards(gidToShards)
			gidToShards[minGid] = append(gidToShards[minGid], shard)

		}

		// 将新的映射关系存储到newConfig中
		var newShards [NShards]int
		for gid, shards := range gidToShards {
			for _, shard := range shards {
				newShards[shard] = gid
			}
		}
		newConfig.Shards = newShards
	}

	csm.Configs = append(csm.Configs, newConfig)

	return OK
}

func (csm *CtrlerStateMachine) Move(shard, gid int) Err {
	lastConfig := csm.Configs[len(csm.Configs)-1]
	newConfig := Config{
		Num:    len(csm.Configs),
		Shards: lastConfig.Shards,
		Groups: copyGroups(lastConfig.Groups),
	}

	newConfig.Shards[shard] = gid
	csm.Configs = append(csm.Configs, newConfig)
	return OK
}

func (csm *CtrlerStateMachine) Query(num int) (Config, Err) {
	if num < 0 || num > len(csm.Configs) {
		return csm.Configs[len(csm.Configs)-1], OK
	}
	return csm.Configs[num], OK
}

func copyGroups(groups map[int][]string) map[int][]string {
	newGroups := make(map[int][]string, len(groups))
	for gid, servers := range groups {
		newGroups[gid] = make([]string, len(servers))
		copy(newGroups[gid], servers)
	}
	return newGroups
}

func gidWithMaxShards(gidToShards map[int][]int) int {
	// 初始 Config[0] 表示没有groups，所有shards分配给无效的group 0（即未分配）
	if shards, ok := gidToShards[0]; ok && len(shards) > 0 {
		return 0 // 作为flag使用，表示初始情况shard未被分配
	}

	// 遍历map时key是无序的
	// 需要保证不同server在自己的applyTask线程中运行gidWithMaxShards得到相同的结果
	// 将 gid 进行排序，确保遍历顺序是确定的
	var gids []int
	for gid := range gidToShards {
		gids = append(gids, gid)
	}
	sort.Ints(gids)

	maxGid, maxShards := -1, -1
	for _, gid := range gids {
		if shards := gidToShards[gid]; len(shards) > maxShards {
			maxGid, maxShards = gid, len(shards)
		}
	}

	return maxGid
}

func gidWithMinShards(gidToShards map[int][]int) int {
	var gids []int
	for gid := range gidToShards {
		gids = append(gids, gid)
	}
	sort.Ints(gids)

	minGid, minShards := -1, NShards+1
	for _, gid := range gids {
		if gid != 0 && len(gidToShards[gid]) < minShards {
			minGid, minShards = gid, len(gidToShards[gid])
		}
	}

	return minGid
}

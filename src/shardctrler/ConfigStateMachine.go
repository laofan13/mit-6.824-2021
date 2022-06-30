package shardctrler

type ConfigStateMachine interface {
	Query(num int) (Config, Err)
	Join(servers map[int][]string) Err
	Leave(gids []int) Err
	Move(shard int, gid int) Err
}

type MemoryConfig struct {
	configs []Config
}

func NewMemoryConfig() *MemoryConfig {
	configs := make([]Config, 1)
	configs[0].Groups = map[int][]string{}
	return &MemoryConfig{configs}
}

func (mc *MemoryConfig) CopyConfig() Config {
	var config Config
	lastConfig := mc.configs[len(mc.configs)-1]

	config.Num = len(mc.configs)
	config.Shards = lastConfig.Shards
	config.Groups = make(map[int][]string, len(lastConfig.Groups))
	for k, v := range lastConfig.Groups {
		servers := make([]string, len(v))
		copy(servers, v)
		config.Groups[k] = servers
	}

	return config
}

func (mc *MemoryConfig) Query(num int) (Config, Err) {
	if num < 0 || num >= len(mc.configs) {
		return mc.configs[len(mc.configs)-1], OK
	}
	return mc.configs[num], OK
}

func (mc *MemoryConfig) Join(servers map[int][]string) Err {
	config := mc.CopyConfig()
	for k, v := range servers {
		config.Groups[k] = v
	}
	// allow re-use of a GID

	mc.reAsignShards(&config)
	mc.configs = append(mc.configs, config)
	DPrintf("latest configs %v", mc.configs)

	return OK
}

func (mc *MemoryConfig) Leave(gids []int) Err {
	config := mc.CopyConfig()
	for _, v := range gids {
		delete(config.Groups, v)
	}
	// allow re-use of a GID
	mc.reAsignShards(&config)
	mc.configs = append(mc.configs, config)
	DPrintf("latest configs %v", mc.configs)
	return OK
}

func (mc *MemoryConfig) Move(shard int, gid int) Err {
	config := mc.CopyConfig()
	config.Shards[shard] = gid

	mc.configs = append(mc.configs, config)

	DPrintf("latest configs %v", mc.configs)
	return OK
}

func (mc *MemoryConfig) reAsignShards(config *Config) {

	if len(config.Groups) == 0 {
		for index, _ := range config.Shards {
			config.Shards[index] = 0
		}
		DPrintf("latest config %v", config)
		return
	}

	mapGid2Shards := make(map[int][]int, len(config.Groups)) // gid -> shards[]
	for k, _ := range config.Groups {
		mapGid2Shards[k] = make([]int, 0)
	}
	// allow re-use of a GID---
	avg := len(config.Shards) / len(config.Groups)

	mapGid2Shards[0] = make([]int, 0)
	for index, gid := range config.Shards {
		if _, ok := mapGid2Shards[gid]; !ok { //if gid is not exist .indicate is already removed
			config.Shards[index] = 0
			gid = 0
		}
		mapGid2Shards[gid] = append(mapGid2Shards[gid], index)
	}

	remaining := mapGid2Shards[0]
	delete(mapGid2Shards, 0)

	keys := make([]int, len(mapGid2Shards))
	for gid, shards := range mapGid2Shards {
		dist := len(shards) - avg
		if dist > 0 {
			remaining = append(remaining, shards[avg:]...)
			mapGid2Shards[gid] = shards[:avg]
		} else if dist < 0 {
			if len(remaining) >= -dist {
				mapGid2Shards[gid] = append(shards, remaining[:-dist]...)
				remaining = remaining[-dist:]
			} else {
				mapGid2Shards[gid] = append(shards, remaining...)
				remaining = remaining[len(remaining):]
				keys = append(keys, gid)
			}
		}
	}

	for _, gid := range keys {
		if shards, ok := mapGid2Shards[gid]; ok {
			dist := avg - len(shards)
			if len(remaining) > dist {
				mapGid2Shards[gid] = append(shards, remaining[:dist]...)
				remaining = remaining[dist:]
			} else {
				mapGid2Shards[gid] = append(shards, remaining...)
				break
			}
		}
	}

	for gid, shards := range mapGid2Shards {
		for _, v := range shards {
			config.Shards[v] = gid
		}
	}
	DPrintf("latest config %v", config)
}

package shardctrler

import "sort"

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
	return OK
}

func (mc *MemoryConfig) Move(shard int, gid int) Err {
	config := mc.CopyConfig()
	config.Shards[shard] = gid

	mc.configs = append(mc.configs, config)

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

	groupLen := len(config.Groups)
	gidList := make([]int, 0, groupLen)
	if groupLen == 0 {
		gidList = []int{0}
	} else {
		for key := range config.Groups {
			gidList = append(gidList, key)
		}
		sort.Ints(gidList)
	}

	for i, _ := range config.Shards {
		config.Shards[i] = gidList[i%groupLen]
	}
}

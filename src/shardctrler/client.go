package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	leaderId  int
	clientId  int64
	commandId int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.clientId = nrand()
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	//client Identify
	args.ClientId = ck.clientId
	args.CommandId = ck.commandId

	for {
		// try each known server.
		var reply GeneticReply
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Query", args, &reply)
		if !ok || reply.Err == WrongLeader || reply.Err == TimeOut {
			// DPrintf("client %v successfully %v {key: %v,value: %v}\n", ck.clientId, request.Op, request.Key, request.Value)
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		ck.commandId++
		return reply.Config
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	//client Identify
	args.ClientId = ck.clientId
	args.CommandId = ck.commandId

	for {
		// try each known server.
		var reply GeneticReply
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Join", args, &reply)
		if !ok || reply.Err == WrongLeader || reply.Err == TimeOut {
			// DPrintf("client %v successfully %v {key: %v,value: %v}\n", ck.clientId, request.Op, request.Key, request.Value)
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		ck.commandId++
		return
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	//client Identify
	args.ClientId = ck.clientId
	args.CommandId = ck.commandId

	for {
		// try each known server.
		var reply GeneticReply
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Leave", args, &reply)
		if !ok || reply.Err == WrongLeader || reply.Err == TimeOut {
			// DPrintf("client %v successfully %v {key: %v,value: %v}\n", ck.clientId, request.Op, request.Key, request.Value)
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		ck.commandId++
		return
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	//client Identify
	args.ClientId = ck.clientId
	args.CommandId = ck.commandId

	for {
		// try each known server.
		var reply GeneticReply
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Move", args, &reply)
		if !ok || reply.Err == WrongLeader || reply.Err == TimeOut {
			// DPrintf("client %v successfully %v {key: %v,value: %v}\n", ck.clientId, request.Op, request.Key, request.Value)
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		ck.commandId++
		return
	}
}

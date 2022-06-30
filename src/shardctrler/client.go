package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	mu        sync.Mutex
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

func (ck *Clerk) getCommandId() int64 {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.commandId++
	return ck.commandId
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num

	//client Identify
	args.ClientId = ck.clientId
	args.CommandId = ck.getCommandId()

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply GeneticReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				DPrintf("client reply a query request with reply %v", reply)
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	//client Identify
	args.ClientId = ck.clientId
	args.CommandId = ck.getCommandId()

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply GeneticReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	//client Identify
	args.ClientId = ck.clientId
	args.CommandId = ck.getCommandId()

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply GeneticReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	//client Identify
	args.ClientId = ck.clientId
	args.CommandId = ck.getCommandId()

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply GeneticReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

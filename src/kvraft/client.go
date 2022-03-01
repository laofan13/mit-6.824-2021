package kvraft

import (
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"math/big"
	"sync"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader int
	mu     sync.Mutex
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
	// You'll have to add code here.
	ck.leader = 0

	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	args := &GetArgs{Key: key}
	args.Id = ck.MD5(args.Key)
	reply := &GetReply{}

	len := len(ck.servers)
	for {

		ok := ck.servers[ck.leader].Call("KVServer.Get", args, reply)
		if ok {
			if reply.Err == OK {
				return reply.Value
			} else if reply.Err == ErrNoKey {
				return ""
			}
		}

		ck.mu.Lock()
		ck.leader = (ck.leader + 1) % len
		ck.mu.Unlock()
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := &PutAppendArgs{Key: key, Value: value, Op: op}
	args.Id = ck.MD5(args.Key + args.Value + args.Op)
	reply := &PutAppendReply{}

	len := len(ck.servers)
	for {
		ok := ck.servers[ck.leader].Call("KVServer.PutAppend", args, reply)
		if ok {
			if reply.Err == OK {
				return
			}
		}
		ck.mu.Lock()
		ck.leader = (ck.leader + 1) % len
		ck.mu.Unlock()
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) MD5(input string) string {
	c := md5.New()
	c.Write([]byte(time.Now().String() + input))
	bytes := c.Sum(nil)
	return hex.EncodeToString(bytes)
}

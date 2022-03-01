package kvraft

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		fmt.Printf(format, a...)
	}
	return
}

type Op_flag int

const (
	Op_GET    Op_flag = 0 //Get
	Op_PUT    Op_flag = 1 //put
	Op_APPEND Op_flag = 2 //Append
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Flag  Op_flag
	Key   string
	Value string

	Id string // unique identfiy
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	logs     map[int]interface{}
	maxIndex int
	data     map[string]string

	visited map[string]bool
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("%v Get args %v\n", kv.me, args)
	op := Op{
		Flag: Op_GET,
		Key:  args.Key,
		Id:   args.Id,
	}

	ok := kv.one(op)
	if !ok {
		reply.Err = ErrWrongLeader
		reply.Value = ""
		return
	}
	kv.mu.Lock()
	val, ok := kv.data[args.Key]
	kv.mu.Unlock()
	if ok {
		reply.Err = OK
		reply.Value = val
	} else {
		reply.Err = ErrNoKey
		reply.Value = ""
	}
	DPrintf("%v GET reply %v\n", kv.me, reply)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintf("%v PutAppend args %v \n", kv.me, args)
	op := Op{
		Key:   args.Key,
		Value: args.Value,
		Id:    args.Id,
	}
	if args.Op == "Put" {
		op.Flag = Op_PUT
	} else if args.Op == "Append" {
		op.Flag = Op_APPEND
	} else {
		reply.Err = ErrWrongLeader
		DPrintf("%v: op %v not found\n", kv.me, args.Op)
		return
	}

	ok := kv.one(op)
	if ok {
		DPrintf("%v apply op %v key %v value %v\n", kv.me, args.Op, op.Key, op.Value)
		reply.Err = OK
	} else {
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) one(op Op) bool {
	kv.mu.Lock()
	if ok := kv.visited[op.Id]; ok {
		kv.mu.Unlock()
		return true
	}
	kv.mu.Unlock()

	t0 := time.Now()
	for time.Since(t0).Seconds() < 10 {
		index := -1
		index1, _, ok := kv.rf.Start(op)
		if ok {
			index = index1
		} else {
			break
		}

		if index != -1 {
			// somebody claimed to be the leader and to have
			// submitted our command; wait a while for agreement.
			t1 := time.Now()
			for time.Since(t1).Seconds() < 2 {
				kv.mu.Lock()
				_, ok1 := kv.logs[index]
				_, ok2 := kv.visited[op.Id]
				kv.mu.Unlock()
				if ok1 {
					// committed
					if ok2 {
						return true
					} else {
						return false
					}

				}
				time.Sleep(20 * time.Microsecond)
			}
			DPrintf("one(%v) failed to Commit\n", op)
			break
		} else {
			time.Sleep(50 * time.Millisecond)
		}
	}

	return false
}

func (kv *KVServer) checkLogs(m raft.ApplyMsg) (string, bool) {
	err_msg := ""
	op := m.Command
	if v, ok := kv.logs[m.CommandIndex]; ok && op != v {
		err_msg = fmt.Sprintf("server %v commit index=%v error\n", kv.me, m.CommandIndex)
	}
	_, prevok := kv.logs[m.CommandIndex-1]
	kv.logs[m.CommandIndex] = op
	if m.CommandIndex > kv.maxIndex {
		kv.maxIndex = m.CommandIndex
	}

	return err_msg, prevok
}

func (kv *KVServer) Applier() {
	for m := range kv.applyCh {
		if m.CommandValid {
			kv.mu.Lock()
			err_msg, prevok := kv.checkLogs(m)
			if m.CommandIndex > 1 && prevok == false {
				err_msg = fmt.Sprintf("server %v apply out of order %v", kv.me, m.CommandIndex)
			}
			if err_msg != "" {
				DPrintf("apply error: %v\n", err_msg)
			}

			op := m.Command.(Op)
			kv.ApplyLog(op)
			kv.visited[op.Id] = true

			kv.mu.Unlock()
		} else {
			// ignore other types of ApplyMsg
		}
	}
}

func (kv *KVServer) ApplyLog(op Op) {
	if op.Flag == Op_PUT {
		kv.data[op.Key] = op.Value
	} else if op.Flag == Op_APPEND {
		val, ok := kv.data[op.Key]
		if ok {
			kv.data[op.Key] = val + op.Value
		} else {
			kv.data[op.Key] = op.Value
		}
	} else if op.Flag == Op_GET {
		// nothing to do
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.data = make(map[string]string)
	kv.logs = make(map[int]interface{})
	kv.visited = make(map[string]bool)

	go kv.Applier()

	return kv
}

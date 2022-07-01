package shardctrler

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	// Your data here.
	lastApplied    int
	lastOperations map[int64]OpLogContext
	notifyChans    map[int]chan *GeneticReply

	//store config
	configStateMachine ConfigStateMachine
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *GeneticReply) {
	DPrintf("Node %v: received a JoinRequest with JoinArgs %v", sc.me, args)

	sc.mu.Lock()
	if sc.isDuplicateRequest(args.ClientId, args.CommandId) {
		lastReply := sc.lastOperations[args.ClientId].lastReply
		reply.Err = lastReply.Err
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	opLog := OpLog{
		Op:      Join,
		Command: *args,
	}

	index, _, isLeader := sc.rf.Start(opLog)
	if !isLeader {
		reply.Err = WrongLeader
		return
	}

	sc.mu.Lock()
	ch := sc.getNotifyChan(index)
	sc.mu.Unlock()

	select {
	case result := <-ch:
		reply.Err = result.Err
	case <-time.After(ExecuteTimeout):
		reply.Err = TimeOut
	}

	// release notifyChan to reduce memory footprint
	// why asynchronously? to improve throughput, here is no need to block client request
	go func() {
		sc.mu.Lock()
		sc.removeOutdatedNotifyChan(index)
		sc.mu.Unlock()
	}()

	DPrintf("Node %v: relpy a JoinRequest with JoinReply %v", sc.me, reply)

}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	DPrintf("Node %v: received a LeaveRequest with LeaveArgs %v", sc.me, args)

	sc.mu.Lock()
	if sc.isDuplicateRequest(args.ClientId, args.CommandId) {
		lastReply := sc.lastOperations[args.ClientId].lastReply
		reply.Err = lastReply.Err
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	opLog := OpLog{
		Op:      Leave,
		Command: *args,
	}

	index, _, isLeader := sc.rf.Start(opLog)
	if !isLeader {
		reply.Err = WrongLeader
		return
	}

	sc.mu.Lock()
	ch := sc.getNotifyChan(index)
	sc.mu.Unlock()

	select {
	case result := <-ch:
		reply.Err = result.Err
	case <-time.After(ExecuteTimeout):
		reply.Err = TimeOut
	}

	// release notifyChan to reduce memory footprint
	// why asynchronously? to improve throughput, here is no need to block client request
	go func() {
		sc.mu.Lock()
		sc.removeOutdatedNotifyChan(index)
		sc.mu.Unlock()
	}()

	DPrintf("Node %v: relpy a LeaveRequest with LeaveReply %v", sc.me, reply)
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	DPrintf("Node %v: received a MoveRequest with MoveArgs %v", sc.me, args)

	sc.mu.Lock()
	if sc.isDuplicateRequest(args.ClientId, args.CommandId) {
		lastReply := sc.lastOperations[args.ClientId].lastReply
		reply.Err = lastReply.Err
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	opLog := OpLog{
		Op:      Move,
		Command: *args,
	}

	index, _, isLeader := sc.rf.Start(opLog)
	if !isLeader {
		reply.Err = WrongLeader
		return
	}

	sc.mu.Lock()
	ch := sc.getNotifyChan(index)
	sc.mu.Unlock()

	select {
	case result := <-ch:
		reply.Err = result.Err
	case <-time.After(ExecuteTimeout):
		reply.Err = TimeOut
	}

	// release notifyChan to reduce memory footprint
	// why asynchronously? to improve throughput, here is no need to block client request
	go func() {
		sc.mu.Lock()
		sc.removeOutdatedNotifyChan(index)
		sc.mu.Unlock()
	}()

	DPrintf("Node %v: relpy a MoveRequest with MoveReply %v", sc.me, reply)
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	DPrintf("Node %v: received a QueryRequest with QueryArgs %v", sc.me, args)

	opLog := OpLog{
		Op:      Query,
		Command: *args,
	}

	index, _, isLeader := sc.rf.Start(opLog)
	if !isLeader {
		reply.Err = WrongLeader
		return
	}

	sc.mu.Lock()
	ch := sc.getNotifyChan(index)
	sc.mu.Unlock()

	select {
	case result := <-ch:
		reply.Err = result.Err
		reply.Config = result.Config
	case <-time.After(ExecuteTimeout):
		reply.Err = TimeOut
	}

	// release notifyChan to reduce memory footprint
	// why asynchronously? to improve throughput, here is no need to block client request
	go func() {
		sc.mu.Lock()
		sc.removeOutdatedNotifyChan(index)
		sc.mu.Unlock()
	}()

	DPrintf("Node %v: relpy a QueryRequest with QueryReply %v index %v", sc.me, reply, index)
}

func (sc *ShardCtrler) isDuplicateRequest(clientId int64, commandId int64) bool {
	operationContext, ok := sc.lastOperations[clientId]
	return ok && commandId <= operationContext.CommandId
}

func (sc *ShardCtrler) getNotifyChan(index int) chan *GeneticReply {
	if _, ok := sc.notifyChans[index]; !ok {
		sc.notifyChans[index] = make(chan *GeneticReply)
	}
	return sc.notifyChans[index]
}

func (sc *ShardCtrler) removeOutdatedNotifyChan(index int) {
	delete(sc.notifyChans, index)
}

func (sc *ShardCtrler) Applier() {
	for !sc.killed() {
		m := <-sc.applyCh
		if m.CommandValid {
			sc.mu.Lock()
			if m.CommandIndex <= sc.lastApplied {
				sc.mu.Unlock()
				continue
			}
			sc.lastApplied = m.CommandIndex
			// get args
			opLog := m.Command.(OpLog)
			argsId := opLog.getArgsId()

			var reply *GeneticReply
			if opLog.Op != Query && sc.isDuplicateRequest(argsId.ClientId, argsId.CommandId) {
				reply = sc.lastOperations[argsId.ClientId].lastReply
			} else {
				reply = sc.applyLogToStateMachine(&opLog)

				if opLog.Op != Query {
					DPrintf("Node %v: update commandId %v,reply %v\n", sc.me, argsId.CommandId, reply)
					sc.lastOperations[argsId.ClientId] = OpLogContext{argsId.CommandId, reply}
				}
			}
			DPrintf("Node %v: received a ApplyMsg with commandId %v and CommandIndex %v", sc.me, argsId.CommandId, m.CommandIndex)

			if currentTerm, isLeader := sc.rf.GetState(); isLeader && m.CommandTerm == currentTerm {
				ch := sc.getNotifyChan(m.CommandIndex)
				ch <- reply
			}

			sc.mu.Unlock()
		} else if m.SnapshotValid {

		} else {
			panic(fmt.Sprintf("unexpected Message %v", m))
		}
	}
}

func (sc *ShardCtrler) applyLogToStateMachine(opLog *OpLog) *GeneticReply {
	var reply GeneticReply
	switch opLog.Op {
	case Join:
		args := opLog.Command.(JoinArgs)
		err := sc.configStateMachine.Join(args.Servers)
		reply.Err = err
	case Leave:
		args := opLog.Command.(LeaveArgs)
		err := sc.configStateMachine.Leave(args.GIDs)
		reply.Err = err
	case Move:
		args := opLog.Command.(MoveArgs)
		err := sc.configStateMachine.Move(args.Shard, args.GID)
		reply.Err = err
	case Query:
		args := opLog.Command.(QueryArgs)
		config, err := sc.configStateMachine.Query(args.Num)
		reply.Err = err
		reply.Config = config
	}

	return &reply
}

func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configStateMachine = NewMemoryConfig()

	labgob.Register(OpLog{})
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(QueryArgs{})

	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.lastOperations = make(map[int64]OpLogContext)
	sc.notifyChans = make(map[int]chan *GeneticReply)

	go sc.Applier()

	return sc
}

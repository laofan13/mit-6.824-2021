package kvraft

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	lastApplied    int
	kvStateMachine KVStateMachine
	lastOperations map[int64]OperationContext
	notifyChans    map[int]chan *CommandResponse
}

func (kv *KVServer) Command(request *CommandRequest, response *CommandResponse) {
	defer DPrintf("{Node %v} processes CommandRequest %v with CommandResponse %v", kv.me, request, response)

	kv.mu.Lock()
	if request.Op != Op_Get && kv.isDuplicateRequest(request.ClientId, request.CommandId) {
		lastResponse := kv.lastOperations[request.ClientId].Response
		response.Value, response.Err = lastResponse.Value, lastResponse.Err
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	index, _, isLeader := kv.rf.Start(Command{request})
	if !isLeader {
		response.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	ch := kv.getNotifyChan(index)
	kv.mu.Unlock()

	select {
	case result := <-ch:
		response.Value, response.Err = result.Value, result.Err
	case <-time.After(ExecuteTimeout):
		response.Err = ErrTimeout
	}
	// release notifyChan to reduce memory footprint
	// why asynchronously? to improve throughput, here is no need to block client request
	go func() {
		kv.mu.Lock()
		kv.removeOutdatedNotifyChan(index)
		kv.mu.Unlock()
	}()

}

func (kv *KVServer) isDuplicateRequest(clientId int64, requestId int64) bool {
	operationContext, ok := kv.lastOperations[clientId]
	return ok && requestId <= operationContext.CommandId
}

func (kv *KVServer) Applier() {
	for !kv.killed() {
		m := <-kv.applyCh
		if m.CommandValid {
			kv.mu.Lock()
			if m.CommandIndex <= kv.lastApplied {
				kv.mu.Unlock()
				continue
			}
			kv.lastApplied = m.CommandIndex
			command := m.Command.(Command)
			var response *CommandResponse

			if command.Op != Op_Get && kv.isDuplicateRequest(command.ClientId, command.CommandId) {
				response = kv.lastOperations[command.ClientId].Response
			} else {
				response = kv.applyLogToStateMachine(&command)
				if command.Op != Op_Get {
					DPrintf("{Node %v} update commandId %v,Response %v\n", kv.me, command.CommandId, response)
					kv.lastOperations[command.ClientId] = OperationContext{command.CommandId, response}
				}
			}

			if currentTerm, isLeader := kv.rf.GetState(); isLeader && m.CommandTerm == currentTerm {
				ch := kv.getNotifyChan(m.CommandIndex)
				ch <- response
			}

			if kv.maxraftstate != -1 && kv.maxraftstate < kv.rf.GetRaftStateSize() {
				kv.rf.Snapshot(m.CommandIndex, kv.readSnapshot())
			}

			kv.mu.Unlock()
		} else if m.SnapshotValid {
			kv.mu.Lock()
			if kv.rf.CondInstallSnapshot(m.SnapshotTerm,
				m.SnapshotIndex, m.Snapshot) {
				kv.readPersist(m.Snapshot)
				kv.lastApplied = m.SnapshotIndex
			}
			kv.mu.Unlock()
		} else {
			panic(fmt.Sprintf("unexpected Message %v", m))
		}
	}
}

func (kv *KVServer) getNotifyChan(index int) chan *CommandResponse {
	if _, ok := kv.notifyChans[index]; !ok {
		kv.notifyChans[index] = make(chan *CommandResponse, 1)
	}
	return kv.notifyChans[index]
}

func (kv *KVServer) removeOutdatedNotifyChan(index int) {
	delete(kv.notifyChans, index)
}

func (kv *KVServer) applyLogToStateMachine(command *Command) *CommandResponse {
	var val string
	var err Err
	switch command.Op {
	case Op_Append:
		err = kv.kvStateMachine.Append(command.Key, command.Value)
	case Op_PUT:
		err = kv.kvStateMachine.Put(command.Key, command.Value)
	case Op_Get:
		val, err = kv.kvStateMachine.Get(command.Key)
	}
	return &CommandResponse{Err: err, Value: val}
}

func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) readSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(kv.lastOperations)
	e.Encode(kv.kvStateMachine)

	return w.Bytes()
}

func (kv *KVServer) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var lastOperations map[int64]OperationContext
	var kvStateMachine MemoryKV
	if d.Decode(&lastOperations) != nil ||
		d.Decode(&kvStateMachine) != nil {
		DPrintf("{Node %v} restores snapshot failed", kv.me)
	}
	kv.lastOperations = lastOperations
	kv.kvStateMachine = &kvStateMachine
}

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Command{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvStateMachine = NewMemoryKV()
	kv.lastOperations = make(map[int64]OperationContext)
	kv.notifyChans = make(map[int]chan *CommandResponse)

	kv.readPersist(persister.ReadSnapshot())

	go kv.Applier()

	DPrintf("{Node %v} has started", kv.me)

	return kv
}

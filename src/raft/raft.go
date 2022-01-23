package raft

import (
	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type NodeState int

const (
	Follower   NodeState = 0
	Candidater NodeState = 1
	Leader     NodeState = 2
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	applyCond sync.Cond
	applyCh   chan ApplyMsg
	state     NodeState

	currentTerm int
	votedFor    int
	log         Log

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	electionTimer *time.Timer
	heartTimer    *time.Timer
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A).
	return rf.currentTerm, rf.state == Leader
}

func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	return w.Bytes()
}

func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	rf.persister.SaveRaftState(rf.encodeState())
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var voteFor int
	var log Log
	if d.Decode(&term) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&log) != nil {
		DPrintf("%v restores persisted state failed", rf.me)
	} else {
		rf.currentTerm = term
		rf.votedFor = voteFor
		rf.log = log
	}

	rf.lastApplied, rf.commitIndex = rf.log.firstIndex(), rf.log.firstIndex()
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("%v service calls CondInstallSnapshot with lastIncludedTerm %v and lastIncludedIndex %v to check whether snapshot is still valid in term %v", rf.me, lastIncludedTerm, lastIncludedIndex, rf.currentTerm)

	// outdated snapshot
	if lastIncludedIndex <= rf.commitIndex {
		DPrintf("%v rejects the snapshot which lastIncludedIndex is %v because commitIndex %v is larger", rf.me, lastIncludedIndex, rf.commitIndex)
		return false
	}

	if lastIncludedIndex > rf.log.lastIndex() {
		rf.log = makeEmptyLog()
		rf.log.StartIndex = lastIncludedIndex
	} else {
		rf.log.nextCuted(lastIncludedIndex)
	}
	rf.log.Logs[0].Term = lastIncludedTerm
	rf.lastApplied, rf.commitIndex = lastIncludedIndex, lastIncludedIndex

	rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)
	DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} after accepting the snapshot which lastIncludedTerm is %v, lastIncludedIndex is %v", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.log.entry(rf.log.firstIndex()), rf.log.entry(rf.log.lastIndex()), lastIncludedTerm, lastIncludedIndex)
	return true
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	snapshotIndex := rf.log.firstIndex()
	if index <= snapshotIndex {
		DPrintf("%v rejects replacing log with snapshotIndex %v as current snapshotIndex %v is larger in term %v", rf.me, index, snapshotIndex, rf.currentTerm)
		return
	}
	rf.log.nextCuted(index)
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)
	DPrintf("%v T:%v Snapshot:{index: %v snapshotIndex: %v,snapshot %v}\n", rf.me, rf.currentTerm, index, snapshotIndex, snapshot)
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	// Your code here (2A, 2B).
	if rf.currentTerm > args.Term || (rf.currentTerm == args.Term && rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}

	if rf.currentTerm < args.Term {
		reply.VoteGranted = true
		rf.state, rf.currentTerm, rf.votedFor = Follower, args.Term, -1
	}

	//比较日志那个新
	lastIndex := rf.log.lastIndex()
	lastTerm := rf.log.lastLog().Term
	isUptoDate := lastTerm < args.LastLogTerm || (lastTerm == args.LastLogTerm && lastIndex <= args.LastLogIndex)

	DPrintf("%v: T %v voteFor %v\n", rf.me, rf.currentTerm, rf.votedFor)
	if !isUptoDate {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}

	rf.votedFor = args.CandidateId
	rf.resetElectionTimeout()
	reply.Term, reply.VoteGranted = rf.currentTerm, true
}

func (rf *Raft) AppendEntries(args *AppendEntriesAags, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	//rule1
	if rf.currentTerm > args.Term {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	if rf.currentTerm < args.Term {
		rf.currentTerm, rf.votedFor = args.Term, -1
	}

	rf.state = Follower
	rf.resetElectionTimeout()

	//rule2
	if args.PrevLogIndex < rf.log.firstIndex() {
		reply.Success = false
		reply.Term = 0
		return
	}

	//rule3
	if rf.log.lastIndex() < args.PrevLogIndex {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.ConflictVaild = true
		reply.ConflictIndex = rf.log.lastIndex() + 1
		reply.ConflictTerm = -1
		return
	}

	//rule4
	if rf.log.entry(args.PrevLogIndex).Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.ConflictVaild = true

		firstIndex := rf.log.firstIndex()
		conflictTerm := rf.log.entry(args.PrevLogIndex).Term
		index := args.PrevLogIndex - 1

		for index >= firstIndex && rf.log.entry(index).Term == conflictTerm {
			index--
		}

		reply.ConflictIndex = index
		reply.ConflictTerm = conflictTerm
		return
	}

	rf.log.AppendLogs(args.PrevLogIndex, args.Entries)

	if rf.commitIndex < args.LeaderCommit {
		rf.commitIndex = args.LeaderCommit
		newCommit := args.PrevLogIndex + len(args.Entries)
		if rf.commitIndex > newCommit {
			rf.commitIndex = newCommit
		}
		DPrintf("%v: commit %v \n", rf.me, rf.commitIndex)
		rf.applyCond.Signal()
	}

	reply.Success = true
	reply.Term = rf.currentTerm
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer DPrintf("%v's state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} before processing InstallSnapshotRequest %v and reply InstallSnapshotResponse %v", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.log.entry(rf.log.firstIndex()), rf.log.entry(rf.log.lastIndex()), args, reply)

	reply.Term = rf.currentTerm

	if rf.currentTerm > args.Term {
		return
	}
	if rf.currentTerm < args.Term {
		rf.currentTerm, rf.votedFor = args.Term, -1
		rf.persist()
	}
	rf.resetElectionTimeout()

	// outdated snapshot
	if args.LastIncludedIndex <= rf.commitIndex {
		return
	}

	go func() {
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
	}()
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesAags, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(peer int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[peer].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2B).

	if rf.state != Leader {
		return -1, -1, false
	}

	e := Entry{rf.currentTerm, command}
	DPrintf("%v: receive a log %+v in Term %v\n", rf.me, e, rf.currentTerm)
	rf.log.append(e)
	rf.persist()
	rf.startAppendEntrys(false)

	return rf.log.lastIndex(), rf.currentTerm, true
}

func (rf *Raft) handleAppendEntries(peer int, args *AppendEntriesAags, reply *AppendEntriesReply) {
	DPrintf("%v AppendEntries reply from %v:%v\n", rf.me, peer, reply)
	if rf.currentTerm == args.Term {
		if reply.Success {
			newNextIndex := args.PrevLogIndex + len(args.Entries) + 1
			newMatchIndex := newNextIndex - 1

			if newNextIndex > rf.nextIndex[peer] {
				rf.nextIndex[peer] = newNextIndex
			}

			if newMatchIndex > rf.matchIndex[peer] {
				rf.matchIndex[peer] = newMatchIndex
			}
			rf.advanceCommit()
			DPrintf("%v: handleAppendEntries success: peer %v nextIndex %v matchIndex %v\n", rf.me, peer, rf.nextIndex[peer], rf.matchIndex[peer])
		} else {
			if rf.currentTerm < reply.Term {
				rf.currentTerm, rf.state, rf.votedFor = reply.Term, Follower, -1
				rf.resetElectionTimeout()
				rf.persist()
			} else if rf.currentTerm == reply.Term && reply.ConflictVaild {
				rf.nextIndex[peer] = reply.ConflictIndex
				if reply.ConflictTerm != -1 {
					firstIndex := rf.log.firstIndex()
					for i := args.PrevLogIndex; i >= firstIndex; i-- {
						if rf.log.entry(i).Term == reply.ConflictTerm {
							rf.nextIndex[peer] = i + 1
							break
						}
					}
				}
			} else {
				if rf.nextIndex[peer] > rf.log.firstIndex()+1 {
					rf.nextIndex[peer] -= 1
				}
			}
		}
	}
}

func (rf *Raft) handleInstallSnapshot(peer int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	if rf.state == Leader && rf.currentTerm == args.Term {
		if rf.currentTerm < reply.Term {
			rf.state = Follower
			rf.currentTerm, rf.votedFor = reply.Term, -1
			rf.resetElectionTimeout()
			rf.persist()
		} else {
			rf.matchIndex[peer], rf.nextIndex[peer] = args.LastIncludedIndex, args.LastIncludedIndex+1
		}
	}
	DPrintf("%v: {State: %v,T: %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} after handling InstallSnapshotResponse %v for InstallSnapshotRequest %v", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.log.firstLog(), rf.log.lastLog(), args, reply)
}

func (rf *Raft) requestAppendEntries(peer int, heartBeat bool) {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	nextIndex := rf.nextIndex[peer]

	if nextIndex-1 < rf.log.firstIndex() {
		// only snapshot can catch up
		DPrintf("%v state:{Term %v firstIndex %v} start InstallSnapshot \n", rf.me, rf.currentTerm, rf.log.firstIndex())
		args := InstallSnapshotArgs{
			Term:              rf.currentTerm,
			LeaderId:          rf.me,
			LastIncludedIndex: rf.log.firstIndex(),
			LastIncludedTerm:  rf.log.firstLog().Term,
			Data:              rf.persister.ReadSnapshot(),
		}
		rf.mu.Unlock()
		reply := InstallSnapshotReply{}
		if rf.sendInstallSnapshot(peer, &args, &reply) {
			rf.mu.Lock()
			rf.handleInstallSnapshot(peer, &args, &reply)
			rf.mu.Unlock()
		}
	} else {
		if nextIndex-1 > rf.log.lastIndex() {
			nextIndex = rf.log.lastIndex()
		}

		args := rf.makeAppendEntriesArgs(nextIndex - 1)
		rf.mu.Unlock()

		DPrintf("%v:requestAppendEntries to %v: %v\n", rf.me, peer, args)

		reply := &AppendEntriesReply{}
		if rf.sendAppendEntries(peer, args, reply) {
			rf.mu.Lock()
			rf.handleAppendEntries(peer, args, reply)
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) makeAppendEntriesArgs(prevLogIndex int) *AppendEntriesAags {
	entries := make([]Entry, rf.log.lastIndex()-prevLogIndex)
	copy(entries, rf.log.nextSlice(prevLogIndex+1))
	return &AppendEntriesAags{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  rf.log.entry(prevLogIndex).Term,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
}

func (rf *Raft) startAppendEntrys(heartBeat bool) {
	if heartBeat {
		DPrintf("%v:start heartBeat T %v\n", rf.me, rf.currentTerm)
	}
	for i := range rf.peers {
		if i != rf.me {
			if rf.log.lastIndex() > rf.nextIndex[i] || heartBeat {
				go rf.requestAppendEntries(i, heartBeat)
			}
		}
	}
}

func (rf *Raft) requestVote(peer int, args *RequestVoteArgs, vote *int) {
	reply := &RequestVoteReply{}

	DPrintf("%v:sendRequestVote to %v: %v\n", rf.me, peer, args)
	if rf.sendRequestVote(peer, args, reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		DPrintf("%v RequestVote reply from %v:%v\n", rf.me, peer, reply)
		if rf.currentTerm == args.Term {
			if reply.VoteGranted {
				*vote++
				if *vote > len(rf.peers)/2 {
					DPrintf("%v: become Leader in Term %v LastIndex %v\n", rf.me, rf.currentTerm, rf.log.lastIndex())
					rf.state = Leader
					rf.startAppendEntrys(true)
					rf.resetHeartTimeout()

					for i := range rf.nextIndex {
						rf.matchIndex[i], rf.nextIndex[i] = 0, rf.log.lastIndex()+1
					}
				}
			} else if rf.currentTerm < reply.Term {
				rf.currentTerm, rf.state, rf.votedFor = reply.Term, Follower, -1
				rf.resetElectionTimeout()
				rf.persist()
			}
		}
	}
}

//send vote to all
func (rf *Raft) StartSelection() {
	DPrintf("%v: tick T %v\n", rf.me, rf.currentTerm)
	vote := 1
	args := RequestVoteArgs{rf.currentTerm, rf.me, rf.log.lastIndex(), rf.log.lastLog().Term}
	for i := range rf.peers {
		if rf.me != i {
			go rf.requestVote(i, &args, &vote)
		}
	}
}

func (rf *Raft) advanceCommit() {
	if rf.state != Leader {
		log.Fatalf("advanceCommit state: %v\n", rf.state)
	}

	start := rf.commitIndex + 1
	if start < rf.log.firstIndex() {
		// start = rf.log.firstIndex()
		return
	}

	for index := start; index <= rf.log.lastIndex(); index++ {
		if rf.log.entry(index).Term != rf.currentTerm {
			continue
		}

		n := 1
		for i, _ := range rf.matchIndex {
			if i != rf.me && rf.matchIndex[i] >= index {
				n++
			}
		}

		if n > len(rf.peers)/2 {
			DPrintf("%v: Commit %v \n", rf.me, index)
			rf.commitIndex = index
		}
	}
	rf.applyCond.Signal()
}

func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for rf.killed() == false {
		if rf.lastApplied < rf.commitIndex && rf.lastApplied < rf.log.lastIndex() && rf.lastApplied+1 > rf.log.firstIndex() {
			rf.lastApplied++
			reply := ApplyMsg{
				CommandValid: true,
				CommandIndex: rf.lastApplied,
				Command:      rf.log.entry(rf.lastApplied).Command,
			}

			DPrintf("%v: applier index: %v\n", rf.me, reply.CommandIndex)

			rf.mu.Unlock()
			rf.applyCh <- reply
			rf.mu.Lock()
		} else {
			rf.applyCond.Wait()
		}
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			if rf.state != Leader {
				rf.currentTerm += 1
				rf.state = Candidater
				rf.votedFor = rf.me
				rf.persist()
				rf.StartSelection()
				rf.resetElectionTimeout()
			}
			rf.mu.Unlock()
		case <-rf.heartTimer.C:
			rf.mu.Lock()
			if rf.state == Leader {
				rf.startAppendEntrys(true) //heartBeat
				rf.resetHeartTimeout()
			}
			rf.mu.Unlock()
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.dead = 0

	// Your initialization code here (2A, 2B, 2C).
	rf.applyCh = applyCh
	rf.state = Follower

	rf.currentTerm = 0
	rf.applyCond = *sync.NewCond(&rf.mu)

	rf.votedFor = -1
	rf.log = makeEmptyLog()

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.electionTimer = time.NewTimer(0)
	rf.heartTimer = time.NewTimer(0)
	rf.resetElectionTimeout()
	rf.resetHeartTimeout()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applier()

	return rf
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) resetElectionTimeout() {
	i := rand.Int31n(1000)
	t := time.Millisecond * time.Duration(i+500)
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(t)
}

func (rf *Raft) resetHeartTimeout() {
	t := time.Millisecond * time.Duration(120)
	rf.heartTimer.Stop()
	rf.heartTimer.Reset(t)
}

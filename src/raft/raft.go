package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//

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

//node state
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

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

func (args RequestVoteArgs) String() string {
	return fmt.Sprintf("%v: T %v lastLogIndex %v lastLogTerm %v", args.CandidateId, args.Term, args.LastLogIndex, args.LastLogTerm)
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (reply RequestVoteReply) String() string {
	return fmt.Sprintf("T %v voteGranted %v", reply.Term, reply.VoteGranted)
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A, 2B).
	if rf.currentTerm > args.Term {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}

	//比较日志那个新
	lastIndex := rf.log.lastIndex()
	lastTerm := rf.log.entry(lastIndex).Term
	isUptoDate := lastTerm < args.LastLogTerm || (lastTerm == args.LastLogTerm && lastIndex <= args.LastLogIndex)

	DPrintf("%v: T %v voteFor %v\n", rf.me, rf.currentTerm, rf.votedFor)
	if !isUptoDate || (rf.currentTerm == args.Term && rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		reply.VoteGranted = false
	} else {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.electionTimer.Reset(RandElectionTimeout())
	}

	reply.Term = rf.currentTerm
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesAags struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	ConflictVaild bool
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) RequestAppendEntries(args *AppendEntriesAags, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//rule1
	if rf.currentTerm > args.Term {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	//rule2
	if rf.log.firstIndex() > args.PrevLogIndex {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	//rule3
	if rf.log.lastIndex() < args.PrevLogIndex {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.ConflictVaild = true
		reply.ConflictIndex = rf.log.lastIndex()
		reply.ConflictTerm = -1
		return
	}

	//rule4
	if rf.log.entry(args.PrevLogIndex).Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.ConflictVaild = true

		index := args.PrevLogIndex
		conflictTerm := rf.log.entry(index).Term

		for ; index >= rf.log.firstIndex(); index-- {
			if rf.log.entry(index).Term != conflictTerm {
				break
			}
		}
		rf.log.preCuted(index)
		reply.ConflictIndex = index + 1
		reply.ConflictTerm = conflictTerm
		return
	}

	rf.log.AppendLogs(args.PrevLogIndex, args.Entries)

	if rf.commitIndex < args.LeaderCommit {
		rf.commitIndex = args.LeaderCommit
		newCommit := args.PrevLogIndex + len(args.Entries)
		if rf.commitIndex > newCommit {
			DPrintf("%v: commit %v \n", rf.me, newCommit)
			rf.commitIndex = newCommit
		}
	}

	reply.Success = true
	reply.Term = rf.currentTerm

	rf.state = Follower
	rf.electionTimer.Reset(RandElectionTimeout())

	rf.applyCond.Signal()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesAags, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.RequestAppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return -1, rf.currentTerm, false
	}

	e := Entry{rf.currentTerm, command}
	rf.log.append(e)
	rf.startAppendEntrys(false)

	// Your code here (2B).

	return rf.log.lastIndex(), rf.currentTerm, true
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) advanceCommit() {
	if rf.state != Leader {
		log.Fatalf("advanceCommit state: %v\n", rf.state)
	}

	start := rf.commitIndex + 1
	if start < rf.log.firstIndex() {
		start = rf.log.firstIndex()
	}

	for index := start; index <= rf.log.lastIndex(); index++ {
		if rf.log.entry(index).Term != rf.currentTerm {
			continue
		}

		n := 1
		for i := range rf.matchIndex {
			if i != rf.me && rf.matchIndex[i] > index {
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

func (rf *Raft) handleAppendEntries(peer int, args *AppendEntriesAags, reply *AppendEntriesReply) {
	DPrintf("%v AppendEntries reply from %v:%v\n", rf.me, peer, reply)
	if rf.currentTerm < reply.Term {
		rf.currentTerm, rf.state, rf.votedFor = reply.Term, Follower, -1
		rf.electionTimer.Reset(RandElectionTimeout())
	} else if rf.currentTerm == args.Term {
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
		} else if reply.ConflictVaild {
			if reply.ConflictIndex > rf.log.lastIndex() {
				rf.nextIndex[peer] = rf.log.lastIndex()
			} else if reply.ConflictIndex < rf.log.firstIndex() {
				rf.nextIndex[peer] = rf.log.firstIndex() + 1
			} else {
				if rf.log.entry(reply.ConflictIndex).Term == reply.ConflictTerm {
					index := reply.ConflictIndex
					for ; index <= rf.log.lastIndex(); index++ {
						if rf.log.entry(index).Term != reply.ConflictTerm {
							break
						}
					}
					rf.nextIndex[peer] = index
				} else {
					rf.nextIndex[peer] = reply.ConflictIndex
				}
			}
		} else if rf.nextIndex[peer] > 1 {
			rf.nextIndex[peer] -= 1
		}
	}
}

func (rf *Raft) requestAppendEntries(peer int, heartBeat bool) {
	rf.mu.Lock()
	nextIndex := rf.nextIndex[peer]

	if nextIndex <= rf.log.firstIndex() {
		nextIndex = rf.log.firstIndex() + 1
	}

	if nextIndex-1 > rf.log.lastIndex() {
		nextIndex = rf.log.lastIndex()
	}

	args := &AppendEntriesAags{rf.currentTerm, rf.me, nextIndex - 1, rf.log.entry(nextIndex - 1).Term,
		make([]Entry, rf.log.lastIndex()-nextIndex+1), rf.commitIndex}

	copy(args.Entries, rf.log.nextSlice(nextIndex))
	rf.mu.Unlock()

	DPrintf("%v:requestAppendEntries to %v: %v\n", rf.me, peer, args)

	reply := &AppendEntriesReply{}
	if rf.sendAppendEntries(peer, args, reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.handleAppendEntries(peer, args, reply)
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
		if rf.currentTerm < reply.Term {
			rf.currentTerm, rf.state, rf.votedFor = reply.Term, Follower, -1
			rf.electionTimer.Reset(RandElectionTimeout())
		}

		if reply.VoteGranted {
			*vote++
			if rf.state == Candidater && rf.currentTerm == args.Term {
				if *vote > len(rf.peers)/2 {
					DPrintf("%v: become Leader in Term %v LastIndex %v\n", rf.me, rf.currentTerm, rf.log.lastIndex())
					rf.state = Leader
					rf.startAppendEntrys(true)
					rf.heartTimer.Reset(RandHeartTimeout())

					for i := range rf.nextIndex {
						rf.nextIndex[i] = rf.log.lastIndex() + 1
					}
				}
			}
		}
	}
}

//send vote to all
func (rf *Raft) StartSelection() {
	DPrintf("%v: tick T %v\n", rf.me, rf.currentTerm)
	vote := 1
	args := RequestVoteArgs{rf.currentTerm, rf.me, rf.log.lastIndex(), rf.log.entry(rf.log.lastIndex()).Term}

	for i := range rf.peers {
		if rf.me != i {
			go rf.requestVote(i, &args, &vote)
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
		case <-rf.heartTimer.C:
			rf.mu.Lock()
			if rf.state == Leader {
				rf.startAppendEntrys(true) //heartBeat
				rf.heartTimer.Reset(RandHeartTimeout())
			}
			rf.mu.Unlock()
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			if rf.state != Leader {
				rf.currentTerm += 1
				rf.state = Candidater
				rf.votedFor = rf.me
				rf.StartSelection()
				rf.electionTimer.Reset(RandElectionTimeout())
			}
			rf.mu.Unlock()
		}
	}
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
				Command:      rf.log.entry(rf.lastApplied),
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

	rf.electionTimer = time.NewTimer(RandHeartTimeout())
	rf.heartTimer = time.NewTimer(RandElectionTimeout())

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applier()

	return rf
}

func RandElectionTimeout() time.Duration {
	i := rand.Int31n(300)
	return time.Millisecond * time.Duration(i+200)
}

func RandHeartTimeout() time.Duration {
	i := rand.Int31n(50)
	return time.Millisecond * time.Duration(i+100)
}

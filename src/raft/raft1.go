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

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const (
	Follower  int = 0
	Candiater int = 1
	Leader    int = 2
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
	applyCh   chan ApplyMsg
	applyCond sync.Cond

	state        int
	electionTime time.Time

	currentTerm int
	voteFor     int
	log         Log

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	return term, isleader
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A, 2B).
	if rf.currentTerm > args.Term {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}

	//比较日志那个新
	lastIndex := rf.log.lastIndex()
	lastTerm := rf.log.LastEntry().Term
	isUptoDate := lastTerm < args.Term || (lastTerm == args.Term && lastIndex <= args.LastLogIndex)

	DPrintf("%v: T %v voteFor %v\n", rf.me, rf.currentTerm, rf.voteFor)
	if (rf.currentTerm < args.Term || rf.voteFor == -1 || rf.voteFor == args.CandidateId) && isUptoDate {
		reply.VoteGranted = true
		rf.voteFor = args.CandidateId
		rf.state = Follower
		rf.resetEletionTime()
	} else {
		reply.VoteGranted = false
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

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
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

func (rf *Raft) RequestAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	//rule1
	if rf.currentTerm > args.Term || rf.log.frontIndex() > args.PrevLogIndex {
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
	if rf.log.getEntry(args.PrevLogIndex).Term != args.Term {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.ConflictVaild = true

		index := args.PrevLogIndex
		conflictTerm := rf.log.getEntry(index).Term

		for ; index >= rf.log.frontIndex(); index-- {
			if rf.log.getEntry(index).Term != conflictTerm {
				break
			}
		}
		reply.ConflictIndex = index + 1
		reply.ConflictTerm = conflictTerm
		return
	}

	rf.log.startSlice(args.PrevLogIndex)
	rf.log.appendEntrys(args.Entries)

	if rf.commitIndex < args.LeaderCommit {
		rf.commitIndex = args.LeaderCommit
		newCommit := args.PrevLogIndex + len(args.Entries)
		if rf.commitIndex > newCommit {
			DPrintf("%v: commit %v \n", rf.me, newCommit)
			rf.commitIndex = newCommit
		}
	}

	rf.applyCond.Signal()
}

func (rf *Raft) sendRequestAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
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

func (rf *Raft) startAppendEntrys(heartBeat bool) {
	for i := range rf.peers {
		if i != rf.me {
			if rf.log.lastIndex() > rf.nextIndex[i] || heartBeat {
				rf.requestAppendEntries(i)
			}
		}
	}
}

func (rf *Raft) requestAppendEntries(peer int) {
	nextIndex := rf.nextIndex[peer]

	if nextIndex <= rf.log.frontIndex() {
		nextIndex = rf.log.frontIndex() + 1
	}

	if nextIndex-1 > rf.log.lastIndex() {
		nextIndex = rf.log.lastIndex()
	}

	args := &AppendEntriesArgs{rf.currentTerm, rf.me, nextIndex - 1, rf.log.getEntry(nextIndex - 1).Term,
		make([]Entry, rf.log.lastIndex()-nextIndex+1), rf.commitIndex}

	copy(args.Entries, rf.log.slice(nextIndex))

	go func() {
		reply := &AppendEntriesReply{}
		if rf.sendRequestAppendEntries(peer, args, reply) {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			rf.handleAppendEntries(peer, args, reply)
		}
	}()

}

func (rf *Raft) handleAppendEntries(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if reply.Term > rf.currentTerm {
		rf.currentTerm, rf.state, rf.voteFor = reply.Term, Follower, -1
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

			DPrintf("%v: handleAppendEntries success: peer %v nextIndex %v matchIndex %v\n", rf.me, peer, rf.nextIndex[peer], rf.matchIndex[peer])
		} else if reply.ConflictVaild {
			if reply.ConflictIndex > rf.log.lastIndex() {
				rf.nextIndex[peer] = rf.log.lastIndex() + 1
			} else if reply.ConflictIndex < rf.log.frontIndex() {
				rf.nextIndex[peer] = rf.log.frontIndex()
			} else {
				if rf.log.getEntry(reply.ConflictIndex).Term == reply.ConflictTerm {
					index := reply.ConflictIndex
					for ; index <= rf.log.lastIndex(); index++ {
						if rf.log.getEntry(index).Term != reply.ConflictTerm {
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
		rf.advanceCommit()
	}
}

func (rf *Raft) advanceCommit() {
	if rf.state != Leader {
		log.Fatalf("advanceCommit state: %v\n", rf.state)
	}

	start := rf.commitIndex + 1
	if start < rf.log.frontIndex() {
		start = rf.log.frontIndex()
	}

	for index := start; index <= rf.log.lastIndex(); index++ {
		if rf.log.getEntry(index).Term != rf.currentTerm {
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

func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for rf.killed() == false {

		if rf.lastApplied < rf.commitIndex && rf.lastApplied < rf.log.lastIndex() && rf.lastApplied+1 > rf.log.frontIndex() {
			rf.lastApplied++
			reply := ApplyMsg{
				CommandValid: true,
				CommandIndex: rf.lastApplied,
				Command:      rf.log.getEntry(rf.lastApplied),
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

func (rf *Raft) requestVote(peer int, args *RequestVoteArgs, vote *int) {
	reply := &RequestVoteReply{}

	DPrintf("%v:sendRequestVote to %v: %v\n", rf.me, peer, args)
	if rf.sendRequestVote(peer, args, reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		DPrintf("%v RequestVote reply from %v:%v\n", rf.me, peer, reply)
		if rf.currentTerm < reply.Term {
			rf.currentTerm, rf.state, rf.voteFor = reply.Term, Follower, -1
		}

		if reply.VoteGranted {
			*vote++
			if rf.state == Candiater && rf.currentTerm == args.Term {
				if *vote >= len(rf.peers)/2 {
					DPrintf("%v: become Leader in Term %v LastIndex %v\n", rf.me, rf.currentTerm, rf.log.lastIndex())
					rf.state = Leader
					rf.startAppendEntrys(true)

					for i := range rf.nextIndex {
						rf.nextIndex[i] = rf.log.lastIndex() + 1
					}
				}
			}
		}
	}
}

func (rf *Raft) startEletion() {
	rf.currentTerm += 1
	rf.state = Candiater
	rf.voteFor = rf.me

	DPrintf("%v: start election in term %v \n", rf.me, rf.currentTerm)

	args := RequestVoteArgs{rf.currentTerm, rf.me, rf.log.lastIndex(), rf.log.LastEntry().Term}
	vote := 1
	for i := range rf.peers {
		if i != rf.me {
			go rf.requestVote(i, &args, &vote)
		}
	}
}

func (rf *Raft) doTick() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("%v: tick state %v \n", rf.me, rf.state)

	if rf.state == Leader {
		rf.resetEletionTime()
		rf.startAppendEntrys(true)
	}

	if time.Now().After(rf.electionTime) {
		rf.resetEletionTime()
		rf.startEletion()
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.doTick()
		time.Sleep(time.Duration(50) * time.Millisecond)
	}
}

func (rf *Raft) resetEletionTime() {
	t := time.Now()
	t.Add(time.Duration(150) * time.Millisecond)
	ms := rand.Int63() % 300
	t.Add(time.Duration(ms) * time.Millisecond)
	rf.electionTime = t
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

	rf.applyCh = applyCh
	rf.applyCond = *sync.NewCond(&rf.mu)

	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follower
	rf.resetEletionTime()

	rf.voteFor = -1
	rf.log = makeEmptyLog()

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applier()

	return rf
}

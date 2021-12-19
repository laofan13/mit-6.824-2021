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
	"math/rand"
	"sort"
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

//log entry

type Entry struct {
	Index   int
	Term    int
	Command interface{}
}

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
	logs        []Entry

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
	var term int
	var isleader bool

	term = rf.currentTerm
	isleader = rf.state == Leader
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

//get last log index
func (rf *Raft) getLastLogIndex() int {
	if len(rf.logs) == 0 {
		return 0
	}
	return rf.logs[len(rf.logs)-1].Index
}

//get first log index
func (rf *Raft) getFirstLogIndex() int {
	if len(rf.logs) == 0 {
		return 0
	}
	return rf.logs[0].Index
}

func (rf *Raft) getLastLogTerm() int {
	if len(rf.logs) == 0 {
		return 0
	}
	return rf.logs[len(rf.logs)-1].Term
}

//get first log index
func (rf *Raft) getFirstLogTerm() int {
	if len(rf.logs) == 0 {
		return 0
	}
	return rf.logs[0].Term
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

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) makeRequestVoteArgs() RequestVoteArgs {
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}
	return args
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("{Node: %v} receives a new {VoteRequest: %v} from {CandidateId Node: %v} with term %v \n", rf.me, args, args.CandidateId, args.Term)

	// 1. 如果`term < currentTerm`返回 false （5.2 节）
	// 2. 如果 votedFor 为空或者为 candidateId，并且候选人的日志至少和自己一样新，那么就投票给他
	if rf.currentTerm > args.Term || (rf.currentTerm == args.Term && rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		DPrintf("{Node %v} Voted against {CandidateId Node: %v} with term %v \n", rf.me, args.CandidateId, rf.currentTerm)
		return
	}

	//如果投票人的日志是否比候选人更新，则投反对票
	lastLogTerm := rf.getLastLogTerm()
	if lastLogTerm > args.LastLogTerm || (lastLogTerm == args.Term && rf.getLastLogIndex() > args.LastLogIndex) {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		DPrintf("The log of {Node %v} is newer than that of {CandidateId Node: %v} with term %v \n", rf.me, args.CandidateId, rf.currentTerm)
		return
	}

	if rf.currentTerm < args.Term {
		rf.state = Follower
		rf.currentTerm, rf.votedFor = args.Term, -1
	}

	rf.votedFor = args.CandidateId
	reply.Term, reply.VoteGranted = rf.currentTerm, true
	rf.electionTimer.Reset(RandElectionTimeout())
	DPrintf("{Node %v} Voted for {Node %v} with term %v\n", rf.me, args.CandidateId, rf.currentTerm)
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
}

func (rf *Raft) makeAppendEntriesAags(peer int) AppendEntriesAags {
	args := AppendEntriesAags{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.nextIndex[peer] - 1,
		LeaderCommit: rf.commitIndex,
	}
	// fmt.Printf("{node %v} AppendEntriesAags %v\n", peer, args)

	if args.PrevLogIndex == 0 {
		args.PrevLogTerm = 0

		logs := make([]Entry, len(rf.logs))
		copy(logs, rf.logs)
		args.Entries = logs
	} else {
		index := args.PrevLogIndex - rf.getFirstLogIndex()
		args.PrevLogTerm = rf.logs[index].Term

		logs := make([]Entry, rf.getLastLogIndex()-args.PrevLogIndex)
		copy(logs, rf.logs[index+1:])
		args.Entries = logs
	}

	return args
}

func (rf *Raft) AppendEntries(args *AppendEntriesAags, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("{Node %v} receives a new {AppendEntriesRequest: %v}from {Node %v} with term %v\n", rf.me, args, args.LeaderId, args.Term)

	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		reply.Success = false
		return
	} else if rf.currentTerm < args.Term {
		rf.currentTerm, rf.votedFor = args.Term, -1
	}
	rf.state = Follower
	rf.electionTimer.Reset(RandElectionTimeout())

	//日志冲突
	if args.PrevLogIndex < rf.getFirstLogIndex() || args.PrevLogIndex > rf.getLastLogIndex() ||
		(args.PrevLogIndex != 0 && rf.logs[args.PrevLogIndex-rf.getFirstLogIndex()].Term != args.PrevLogTerm) {
		reply.Success = false
		return
	}

	if len(args.Entries) != 0 {
		if args.PrevLogIndex == 0 {
			rf.logs = append(rf.logs[:args.PrevLogIndex-rf.getFirstLogIndex()], args.Entries...)
		} else {
			rf.logs = append(rf.logs[:args.PrevLogIndex-rf.getFirstLogIndex()+1], args.Entries...)
		}
	}

	newCommitIndex := rf.getLastLogIndex()
	if newCommitIndex > args.LeaderCommit {
		newCommitIndex = args.LeaderCommit
	}
	if newCommitIndex > rf.commitIndex {
		rf.commitIndex = newCommitIndex
		rf.applyCond.Signal()
	}

	fmt.Printf("{Node %v} logs = {%+v}\n", rf.me, rf.logs)

	reply.Success = true
	DPrintf("{Node %v} Send a {AppendEntriesResponse: %v} to {Node %v} with term %v\n", rf.me, reply, args.LeaderId, args.Term)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesAags, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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

	index := rf.getLastLogIndex() + 1
	term := rf.currentTerm
	isLeader := rf.state == Leader

	// Your code here (2B).
	e := Entry{
		Index:   index,
		Term:    term,
		Command: command,
	}
	if isLeader {
		rf.AppendLog(e)
		go rf.ProcessLog()
	}

	return index, term, isLeader
}

func (rf *Raft) AppendLog(entry Entry) {
	fmt.Printf("{Node %v} receive a {Command %+v} from client\n", rf.me, entry)
	rf.nextIndex[rf.me] = entry.Index + 1
	rf.matchIndex[rf.me] = entry.Index
	rf.logs = append(rf.logs, entry)
}

func (rf *Raft) ProcessLog() {
	for i, _ := range rf.peers {
		if rf.me != i && !rf.killed() {
			go func(peer int) {
				args := rf.makeAppendEntriesAags(peer)
				reply := AppendEntriesReply{}
				fmt.Printf("{Node %v} send a {AppendEntries: %+v} to {Node %v} with term %v\n", rf.me, args, peer, args.Term)
				if rf.sendAppendEntries(peer, &args, &reply) {
					rf.mu.Lock()
					rf.handleAppendEntries(peer, &args, &reply)
					rf.mu.Unlock()
				}
			}(i)
		}
	}
}

func (rf *Raft) handleAppendEntries(peer int, args *AppendEntriesAags, reply *AppendEntriesReply) {
	fmt.Printf("{Node %v} receive {reply: %+v} after send a {args: %+v} with term %v\n", rf.me, reply, args, args.Term)
	if rf.state == Leader && rf.currentTerm == args.Term {
		if reply.Success {
			if len(args.Entries) != 0 {
				// fmt.Printf("{Node %v} append sucessfully with term %v", peer, args.Term)
				rf.nextIndex[peer] = args.Entries[len(args.Entries)-1].Index + 1
				rf.matchIndex[peer] = rf.nextIndex[peer] - 1
				fmt.Printf("{leader %v} update {nextIndex %v } and {matchIndex %v } in {Node %v}\n", rf.me, rf.nextIndex[peer], rf.matchIndex[peer], peer)
				rf.updateCommitIndex()
			}
		} else {
			if rf.currentTerm < reply.Term {
				DPrintf("{Node %v} finds a new leader {Node %v} with term %v and steps down in term %v\n", rf.me, peer, args.Term, rf.currentTerm)
				rf.state, rf.currentTerm, rf.votedFor = Follower, reply.Term, -1
				rf.electionTimer.Reset(RandElectionTimeout())
			} else {
				rf.nextIndex[peer]--
			}
		}
	}
}

func (rf *Raft) updateCommitIndex() {
	n := len(rf.matchIndex)
	srt := make([]int, n)
	copy(srt, rf.matchIndex)
	sort.Ints(srt)
	fmt.Printf("%v\n", srt)
	newCommitIndex := srt[n-(n/2+1)]

	if newCommitIndex > rf.commitIndex {
		if rf.logs[newCommitIndex-rf.getFirstLogIndex()].Term == rf.currentTerm {
			rf.commitIndex = newCommitIndex
			rf.applyCond.Signal()
		}
	}
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

func (rf *Raft) StartBroadCastHeart() {
	DPrintf("{Node %v} start Send heart with term %v\n", rf.me, rf.currentTerm)
	for i, _ := range rf.peers {
		if i != rf.me && !rf.killed() {
			go func(peer int) {
				rf.mu.Lock()
				args := rf.makeAppendEntriesAags(peer)
				rf.mu.Unlock()
				reply := AppendEntriesReply{}

				if rf.sendAppendEntries(peer, &args, &reply) {
					rf.mu.Lock()
					rf.handleAppendEntries(peer, &args, &reply)
					rf.mu.Unlock()
				}
			}(i)
		}
	}
}

//send vote to all
func (rf *Raft) StartSelection() {
	rf.mu.Lock()
	DPrintf("{Node %v} starts election in %v term\n", rf.me, rf.currentTerm)
	rf.currentTerm += 1
	rf.state = Candidater
	rf.votedFor = rf.me
	vote := 1
	args := rf.makeRequestVoteArgs()
	rf.mu.Unlock()

	for i := range rf.peers {
		if rf.me != i && !rf.killed() {
			go func(peer int) {
				reply := RequestVoteReply{}
				if rf.sendRequestVote(peer, &args, &reply) {
					rf.mu.Lock()
					rf.handleRequestVote(&vote, peer, &args, &reply)
					rf.mu.Unlock()
				}
			}(i)
		}
	}
}

func (rf *Raft) handleRequestVote(vote *int, peer int, args *RequestVoteArgs, reply *RequestVoteReply) {
	if rf.currentTerm == args.Term && rf.state == Candidater {
		if reply.VoteGranted {
			DPrintf("{Node %v} get a vote from {Node %v} with term %v\n", rf.me, peer, args.Term)
			*vote++
			if *vote > len(rf.peers)/2 {
				DPrintf("{Node %v} Win elect in term %v\n", rf.me, rf.currentTerm)
				rf.state = Leader
				rf.StartBroadCastHeart()
				rf.heartTimer.Reset(RandHeartTimeout())

				//初始化所有的 nextIndex 值为自己的最后一条日志的 index 加1
				for i, _ := range rf.nextIndex {
					rf.nextIndex[i] = rf.getLastLogIndex() + 1
				}
			}
		} else if reply.Term > rf.currentTerm {
			DPrintf("{Node %v} finds a new leader {Node %v} with term %v and steps down in term %v\n", rf.me, peer, args.Term, rf.currentTerm)
			rf.state, rf.currentTerm, rf.votedFor = Follower, reply.Term, -1
			rf.electionTimer.Reset(RandElectionTimeout())
		}
	}
}

func (rf *Raft) applyLog() {
	for rf.killed() == false {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		firstIndex, commitIndex, lastApplied := rf.getFirstLogIndex(), rf.commitIndex, rf.lastApplied
		entrys := make([]Entry, commitIndex-lastApplied)
		copy(entrys, rf.logs[lastApplied-firstIndex+1:commitIndex-firstIndex+1])
		rf.mu.Unlock()

		for _, e := range entrys {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      e.Command,
				CommandIndex: e.Index,
				CommandTerm:  e.Term,
			}
		}

		rf.mu.Lock()
		rf.lastApplied = commitIndex
		rf.mu.Unlock()
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
				rf.StartBroadCastHeart()
				rf.heartTimer.Reset(RandHeartTimeout())
			}
			rf.mu.Unlock()
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			if rf.state != Leader {
				rf.mu.Unlock()
				rf.StartSelection()
				rf.electionTimer.Reset(RandElectionTimeout())
			} else {
				rf.mu.Unlock()
			}
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

	rf.mu = sync.Mutex{}
	rf.applyCond = *sync.NewCond(&rf.mu)
	rf.currentTerm = 0
	rf.votedFor = -1

	rf.logs = []Entry{}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.electionTimer = time.NewTimer(RandHeartTimeout())
	rf.heartTimer = time.NewTimer(RandElectionTimeout())

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applyLog()

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

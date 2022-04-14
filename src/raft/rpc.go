package raft

import (
	"fmt"
	"time"
)

const ElectionTimeout = 200 * time.Millisecond

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

func (ns NodeState) String() string {
	switch ns {
	case Follower:
		return "Follower"
	case Candidater:
		return "Candidater"
	case Leader:
		return "Leader"
	}
	panic("Not Found NodeState")
}

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// func (args *RequestVoteArgs) String() string {
// 	return fmt.Sprintf("{ Term: %v Id: %v LastLogIndex: %v LastLogTerm: %v }", args.CandidateId, args.Term, args.LastLogIndex, args.LastLogTerm)
// }

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (reply *RequestVoteReply) String() string {
	return fmt.Sprintf("{Term: %v VoteGranted: %v}", reply.Term, reply.VoteGranted)
}

type AppendEntriesAags struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

// func (args *AppendEntriesAags) String() string {
// 	return fmt.Sprintf("{Term: %v LeaderId: %v PrevLogIndex: %v PrevLogTerm: %v len(logs): %v LeaderCommit: %v}", args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries), args.LeaderCommit)
// }

type AppendEntriesReply struct {
	Term    int
	Success bool

	ConflictVaild bool
	ConflictIndex int
	ConflictTerm  int
}

// func (reply *AppendEntriesReply) String() string {
// 	return fmt.Sprintf("{Term: %v Success: %v ConflictVaild: %v ConflictIndex: %v ConflictTerm: %v}", reply.Term, reply.Success, reply.ConflictVaild, reply.ConflictIndex, reply.ConflictTerm)
// }

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

// func (args *InstallSnapshotArgs) String() string {
// 	return fmt.Sprintf("{Term: %v LeaderId: %v LastIncludedIndex: %v LastIncludedTerm: %v}", args.Term, args.LeaderId, args.LastIncludedIndex, args.LastIncludedTerm)
// }

type InstallSnapshotReply struct {
	Term int
}

package shardctrler

import (
	"log"
	"time"
)

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const ExecuteTimeout = 1000 * time.Millisecond

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

type Err int

const (
	OK Err = iota
	WrongLeader
	TimeOut
)

func (err Err) String() string {
	switch err {
	case OK:
		return "OK"
	case WrongLeader:
		return "WrongLeader"
	case TimeOut:
		return "TimeOut"
	}
	panic("Not Found Err ")
}

type ArgsID struct {
	ClientId  int64
	CommandId int64
}

type JoinArgs struct {
	Servers map[int][]string // new GID -> servers mappings
	ArgsID
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs []int
	ArgsID
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard int
	GID   int
	ArgsID
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num int // desired config number
	ArgsID
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

type GeneticReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

type Op int

const (
	Join Op = iota
	Leave
	Move
	Query
)

func (op Op) String() string {
	switch op {
	case Join:
		return "Join"
	case Leave:
		return "Leave"
	case Move:
		return "Move"
	case Query:
		return "Query"
	}
	panic("Not Found Op Flag")
}

type OpLog struct {
	// Your data here.
	Op      Op
	Command interface{}
}

func (op *OpLog) getArgsId() ArgsID {
	var clientId int64
	var commandId int64
	switch op.Op {
	case Join:
		args := op.Command.(JoinArgs)
		clientId, commandId = args.ClientId, args.CommandId
	case Leave:
		args := op.Command.(LeaveArgs)
		clientId, commandId = args.ClientId, args.CommandId
	case Move:
		args := op.Command.(MoveArgs)
		clientId, commandId = args.ClientId, args.CommandId
	case Query:
		args := op.Command.(QueryArgs)
		clientId, commandId = args.ClientId, args.CommandId
	default:
		panic("Not Found Op Flag")
	}
	return ArgsID{
		clientId, commandId,
	}
}

type OpLogContext struct {
	CommandId int64
	lastReply *GeneticReply
}

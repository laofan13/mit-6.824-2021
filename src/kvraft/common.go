package kvraft

import "fmt"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Id string
}

func (args *PutAppendArgs) String() string {
	return fmt.Sprintf("Id: %v Key: %v Value: %v Op: %v", args.Id, args.Key, args.Value, args.Op)
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Id string
}

func (args *GetArgs) String() string {
	return fmt.Sprintf("Id: %v Key: %v", args.Id, args.Key)
}

type GetReply struct {
	Err   Err
	Value string
}

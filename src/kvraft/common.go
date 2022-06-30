package kvraft

import (
	"fmt"
	"log"
	"time"
)

const ExecuteTimeout = 500 * time.Millisecond

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Operation int

const (
	Op_PUT Operation = iota
	Op_Append
	Op_Get
)

func (op Operation) String() string {
	switch op {
	case Op_PUT:
		return "PUT"
	case Op_Append:
		return "APPEND"
	case Op_Get:
		return "GET"
	}
	panic("Not Found Operation")
}

type Err int

const (
	OK Err = iota
	ErrNoKey
	ErrWrongLeader
	ErrTimeout
)

func (err Err) String() string {
	switch err {
	case OK:
		return "OK"
	case ErrNoKey:
		return "ErrNoKey"
	case ErrWrongLeader:
		return "ErrWrongLeader"
	case ErrTimeout:
		return "Timeout"
	}
	panic("Not Found Err ")
}

type Command struct {
	*CommandRequest
}

type CommandRequest struct {
	Key   string
	Value string
	Op    Operation

	ClientId  int64
	CommandId int64
}

func (args *CommandRequest) String() string {
	return fmt.Sprintf("clientId: %v,CommandId: %v, Key: %v, Value: %v, Op: %v", args.ClientId, args.CommandId, args.Key, args.Value, args.Op)
}

type CommandResponse struct {
	Err   Err
	Value string
}

func (args *CommandResponse) String() string {
	return fmt.Sprintf("Value: %v,Err: %v\n", args.Value, args.Err)
}

type OperationContext struct {
	CommandId int64
	Response  *CommandResponse
}

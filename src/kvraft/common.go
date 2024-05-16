package kvraft

import (
	"log"
)

// const Debug = false

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.SetFlags(log.Lmicroseconds)
		log.Printf(format, a...)
	}
	return
}

const (
	OK             Err = "OK"
	ErrNoKey       Err = "ErrNoKey"
	ErrWrongLeader Err = "ErrWrongLeader"
)
const (
	OPGET    = "Get"
	OPPUT    = "Put"
	OPAPPEND = "Append"
	OPDELETE = "Delete"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	Op      string // "Put" or "Append"
	UUID    int64
	PreUUID int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	UUID    int64
	PreUUID int64
}

type GetReply struct {
	Err   Err
	Value string
}

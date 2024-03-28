package kvsrv

import (
	"crypto/rand"
	"math/big"

	"6.5840/labrpc"
)

type Clerk struct {
	server *labrpc.ClientEnd
	// You will have to modify this struct.

	// Your scheme for duplicate detection should free server memory quickly,
	// for example by having each RPC imply that the client has seen the reply for its previous RPC.
	// 因为只有一个server，client操作失败后会不断retry。所以可以认为client的这一次操作时已经完成了上一次操作。
	lastOpUUID int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	// You'll have to add code here.
	ck.lastOpUUID = -1
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := GetArgs{
		Key:      key,
		UUID:     nrand(),
		LastUUID: ck.lastOpUUID,
	}
	reply := GetReply{}
	for ok := ck.server.Call("KVServer.Get", &args, &reply); !ok; ok = ck.server.Call("KVServer.Get", &args, &reply) {
		// DPrintf("Get Retry %v.\n", key)
	}
	ck.lastOpUUID = args.UUID
	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		UUID:     nrand(),
		LastUUID: ck.lastOpUUID,
	}
	reply := PutAppendReply{}
	for ok := ck.server.Call("KVServer."+op, &args, &reply); !ok; ok = ck.server.Call("KVServer."+op, &args, &reply) {
	}
	ck.lastOpUUID = args.UUID
	return reply.Value
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}

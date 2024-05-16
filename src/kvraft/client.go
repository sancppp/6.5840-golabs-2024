package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.5840/labrpc"
)

var cnt = 0

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	//TODO Clerk要加什么字段？
	preUUID   int64
	id        int
	curLeader int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	DPrintf("Clerk MakeClerk,len(servers) = %v\n", len(servers))
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	//TODO
	ck.id = cnt
	cnt++
	ck.curLeader = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	// Get请求也只能在Leader节点中进行
	// 如果rand到一个不在过半分区中的节点，则会拿到错误的值
	// 所以，将Get请求也视为一个需要达到共识的请求，确保Get到的是最新值
	args := &GetArgs{
		Key:     key,
		UUID:    nrand(),
		PreUUID: ck.preUUID,
	}
	reply := &GetReply{}
	for ; ; ck.curLeader = (ck.curLeader + 1) % len(ck.servers) {
		if ok := ck.servers[ck.curLeader].Call("KVServer.Get", args, reply); ok {
			switch reply.Err {
			case OK:
				{
					DPrintf("Clerk[%v] Get: Leaderindex = %v,k/v = %v/%v,UUID = %v", ck.id, ck.curLeader, key, reply.Value, args.UUID)
					ck.preUUID = args.UUID
					return reply.Value
				}
			case ErrNoKey:
				{
					ck.preUUID = args.UUID
					return "" //returning the empty string for non-existent keys
				}
			case ErrWrongLeader:
				continue //非Leader
			}
		}
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := &PutAppendArgs{
		Key:     key,
		Value:   value,
		Op:      op,
		UUID:    nrand(),
		PreUUID: ck.preUUID,
	}
	reply := &PutAppendReply{}
	for ; ; ck.curLeader = (ck.curLeader + 1) % len(ck.servers) {
		// DPrintf("Clerk[%v] %v: i= %v, UUID = %v", ck.name, op, i, args.UUID)
		if ok := ck.servers[ck.curLeader].Call("KVServer."+op, args, reply); ok && reply.Err == OK {
			DPrintf("Clerk[%v] %v: Leaderindex = %v, k/v = %v/%v, UUID = %v", ck.id, op, ck.curLeader, key, value, args.UUID)
			ck.preUUID = args.UUID
			return
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, OPPUT)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, OPAPPEND)
}

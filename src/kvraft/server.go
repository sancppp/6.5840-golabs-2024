package kvraft

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

// Op是raft中logEntry的主体
type Op struct {
	Key       string
	Value     string
	Operation string
	UUID      int64
	Index     int
}

// KVServer要与Raft进行交互！！！
type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	result          map[int64]string  //UUID -> result的map，防止重复执行.需要共识。TODO：需要删去已经确认完成的请求
	data            map[string]string //维护一个map，作为数据库。需要共识
	lastStartIndex  int               //记录最后一个apply的index，防止读操作时之前的写操作还没达成共识
	lastFinishIndex int               //记录最后一个达成共识的index
}

func (kv *KVServer) commonHandler(PreUUID int64, UUID int64, key string, value string, op string) (Err, string) {

	if _, isleader := kv.rf.GetState(); !isleader {
		return ErrWrongLeader, ""
	}

	kv.mu.Lock()

	//如果这个请求已经做了，直接返回
	if val, ok := kv.result[UUID]; ok {
		if val != "-1" {
			kv.mu.Unlock()
			return OK, val
		} else {
			//这个请求已经做了，但是没还做完
			fmt.Printf("repeat+doing UUID = %v, val: %v\n", UUID, val)
			// return ErrWrongLeader, ""
		}
	} else {
		kv.result[UUID] = "-1"
	}

	// 此时，这个节点是leader，需要Start这个op，并且等待其达成共识，最后返回
	command := Op{
		Key:       key,
		Value:     value,
		Operation: op,
		UUID:      UUID,
		Index:     kv.lastStartIndex + 1,
	}
	index, term, isleader := kv.rf.Start(command)
	if !isleader {
		kv.mu.Unlock()
		return ErrWrongLeader, ""
	}
	kv.lastStartIndex = index
	DPrintf("AServer[%v] %v: k/v =%v/%v , UUID:%v", kv.me, command.Operation, command.Key, command.Value, command.UUID)
	kv.mu.Unlock()

	timeoutTime := 10
	time.Sleep(15 * time.Millisecond)
	for timeoutTime > 0 {
		kv.mu.Lock()
		// 检查是否已经达成共识
		if val, ok := kv.result[UUID]; ok && val != "-1" {
			kv.mu.Unlock()
			return OK, val
		}

		//还未达成共识
		nowTerm, isLeder := kv.rf.GetState()
		//请求并未完成，但是发现本raft节点已经不是leader节点，或者发现本节点的状态已经和当时发起共识时不一样了则此次动作作废，以上几种情况均是失败的，直接返回共识错误给客户端
		if nowTerm != term || !isLeder {
			kv.mu.Unlock()
			return ErrWrongLeader, ""
		}
		// 状态正常，睡眠等待
		timeoutTime--
		kv.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
	log.Printf("!!! timeout AServer[%v] %v: k/v =%v/%v , UUID:%v", kv.me, command.Operation, command.Key, command.Value, command.UUID)
	return ErrWrongLeader, ""
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if _, isleader := kv.rf.GetState(); !isleader {
		reply.Err = ErrWrongLeader
		return
	}
	tmpindex := kv.lastStartIndex
	//等待这次读请求之前的写操作达成共识
	for kv.lastFinishIndex < tmpindex {
		DPrintf("...server[%v] wait %v %v %v", kv.me, kv.lastFinishIndex, tmpindex, args.UUID)
		time.Sleep(10 * time.Millisecond)
	}
	reply.Err, reply.Value = kv.commonHandler(args.PreUUID, args.UUID, args.Key, "", OPGET)
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	reply.Err, _ = kv.commonHandler(args.PreUUID, args.UUID, args.Key, args.Value, OPPUT)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	reply.Err, _ = kv.commonHandler(args.PreUUID, args.UUID, args.Key, args.Value, OPAPPEND)
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) handlerApplyCh() {
	for e := range kv.applyCh {
		if e.CommandValid {
			command := e.Command.(Op)
			//执行command
			switch command.Operation {
			case OPGET:
				{
					result, ok2 := kv.data[command.Key]
					if ok2 {
						command.Value = result
					} else {
						command.Value = ""
					}
				}
			case OPPUT:
				{
					kv.data[command.Key] = command.Value
				}
			case OPAPPEND:
				{
					kv.data[command.Key] += command.Value
				}
			case OPDELETE:
				{
					delete(kv.result, command.UUID)
					continue
				}
			}
			DPrintf("Server[%v] %v: k/v =%v/%v , UUID:%v", kv.me, command.Operation, command.Key, command.Value, command.UUID)
			kv.mu.Lock()
			//Get操作的返回值
			kv.result[command.UUID] = command.Value

			kv.lastFinishIndex = command.Index
			//TODO 定期进行
			// kv.rf.Snapshot()
			kv.mu.Unlock()
		} else if e.SnapshotValid {
			//安装Leader发来的快照
		}
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.data = map[string]string{}
	kv.result = map[int64]string{}

	// You may need initialization code here.

	go kv.handlerApplyCh()

	return kv
}

package kvsrv

import (
	"log"
	"runtime"
	"sync"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	data        map[string]string
	once        map[int64]bool
	returnValue map[int64]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.data[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// 如果这个UUID还没有被执行过，则执行。
	// 如果这次操作的UUID已经被执行过了，则不再次执行
	if !kv.once[args.UUID] {
		kv.data[args.Key] = args.Value
		kv.once[args.UUID] = true
	}
	if args.LastUUID != -1 {
		delete(kv.once, args.LastUUID)
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	kv.mu.Lock()
	defer kv.mu.Unlock()
	//正确性：如果append被重发了，如何处理返回值？
	// 同put操作，保证一次操作只被执行一次。
	if !kv.once[args.UUID] {
		kv.returnValue[args.UUID] = kv.data[args.Key] //记录这次Append操作正确的返回值
		kv.data[args.Key] += args.Value
		kv.once[args.UUID] = true
	}
	if args.LastUUID != -1 {
		delete(kv.once, args.LastUUID)
		delete(kv.returnValue, args.LastUUID)
	}
	reply.Value = kv.returnValue[args.UUID]
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.data = map[string]string{}
	kv.once = map[int64]bool{}
	kv.returnValue = map[int64]string{}
	// 很蠢的办法，delete map并不会马上释放内存。开个协程定期GC
	go func(T int) {
		t := time.NewTicker(time.Duration(T) * time.Millisecond)
		for range t.C {
			runtime.GC()
		}
	}(70)
	return kv
}

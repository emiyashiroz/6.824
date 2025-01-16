package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.RWMutex

	// Your definitions here.
	m    map[string]string // kv
	reqM map[string]string //
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	v, ok := kv.m[args.Key]
	if ok {
		reply.Value = v
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.UUID = args.UUID
	_, ok := kv.reqM[args.UUID]
	// 已经执行过了
	if ok {
		value, _ := kv.m[args.Key]
		reply.Value = value
		return
	}
	kv.m[args.Key] = args.Value
	kv.reqM[args.UUID] = ""
	reply.Value = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.UUID = args.UUID
	oldV, ok := kv.reqM[args.UUID]
	// 已经执行过了
	if ok {
		reply.Value = oldV
		return
	}
	v, ok := kv.m[args.Key]
	if ok {
		reply.Value = v
		kv.m[args.Key] = v + args.Value
		kv.reqM[args.UUID] = v
	} else {
		kv.m[args.Key] = args.Value
		kv.reqM[args.UUID] = ""
	}
}

func (kv *KVServer) DeleteUUID(args *DeleteUUIDArgs, reply *DeleteUUIDReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	delete(kv.reqM, args.UUID)
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.m = make(map[string]string)
	kv.reqM = make(map[string]string)
	return kv
}

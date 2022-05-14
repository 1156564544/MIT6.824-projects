package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType    string
	Key       string
	Value     string
	RequestId int
	ClientId  int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvDB          map[string]string
	ch            map[int]chan Op // index(raft) -> chan(Op)
	lastrequestId map[int64]int   // clientId -> last requestId

	lastSSRaftIndex int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Is KVServer is dead?
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	// Is the Raft is a leader?
	if _, isleader := kv.rf.GetState(); !isleader {
		reply.Err = ErrWrongLeader
		return
	}
	// request the operation to Raft
	op := Op{OpType: "Get", Key: args.Key, RequestId: args.RequestId, ClientId: args.ClientId}
	raftIndex, _, _ := kv.rf.Start(op)
	// initialize the channel which the operation will be sent to do
	kv.mu.Lock()
	waitCh, exist := kv.ch[raftIndex]
	if !exist {
		kv.ch[raftIndex] = make(chan Op, 1)
		waitCh = kv.ch[raftIndex]
	}
	kv.mu.Unlock()
	reply.Err = OK
	// waite for the operation to be applied
	select {
	// receive the channel
	case op = <-waitCh:
		if op.RequestId == args.RequestId && op.ClientId == args.ClientId {
			value, exist := kv.DoGet(op)
			if exist {
				reply.Value = value
			} else {
				reply.Err = ErrNoKey
				reply.Value = ""
			}
		} else {
			reply.Err = ErrWrongLeader
		}
	// timeout
	case <-time.After(time.Duration(consensusTimeout * time.Millisecond)):
		_, isleader := kv.rf.GetState()
		if kv.isRequestDuplicated(op.ClientId, op.RequestId) && isleader {
			value, exist := kv.DoGet(op)
			if exist {
				reply.Value = value
			} else {
				reply.Err = ErrNoKey
				reply.Value = ""
			}
		} else {
			reply.Err = ErrWrongLeader
		}
	}
	go func() {
		kv.mu.Lock()
		delete(kv.ch, raftIndex)
		kv.mu.Unlock()
	}()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// Is KVServer is dead?
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	// Is the Raft is a leader?
	if _, isleader := kv.rf.GetState(); !isleader {
		reply.Err = ErrWrongLeader
		return
	}
	// request the operation to Raft
	op := Op{OpType: args.Op, Key: args.Key, Value: args.Value, RequestId: args.RequestId, ClientId: args.ClientId}
	raftIndex, _, _ := kv.rf.Start(op)
	// initialize the channel which the operation will be sent to do
	kv.mu.Lock()
	waitCh, exist := kv.ch[raftIndex]
	if !exist {
		kv.ch[raftIndex] = make(chan Op, 1)
		waitCh = kv.ch[raftIndex]
	}
	kv.mu.Unlock()
	reply.Err = OK
	// waite for the operation to be applied
	select {
	// receive the channel
	case op = <-waitCh:
		if op.RequestId == args.RequestId && op.ClientId == args.ClientId {
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	// timeout
	case <-time.After(time.Duration(consensusTimeout * time.Millisecond)):
		_, isleader := kv.rf.GetState()
		if kv.isRequestDuplicated(op.ClientId, op.RequestId) && isleader {
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	}
	go func() {
		kv.mu.Lock()
		delete(kv.ch, raftIndex)
		kv.mu.Unlock()
	}()
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
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
//
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

	// You may need initialization code here.
	kv.kvDB = make(map[string]string)
	kv.ch = make(map[int]chan Op)
	kv.lastrequestId = make(map[int64]int)

	go kv.waitApplyCh()
	return kv
}

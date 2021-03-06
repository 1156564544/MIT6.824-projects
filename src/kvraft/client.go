package kvraft

import "6.824/labrpc"
import "crypto/rand"
import "math/big"
import mathrand "math/rand"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientId  int64
	requestId int
	leaderId  int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.requestId = 0
	ck.leaderId = mathrand.Intn(len(servers))
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	ck.requestId++
	requestId := ck.requestId
	server := ck.leaderId
	args := GetArgs{Key: key, ClientId: ck.clientId, RequestId: requestId}

	for {
		reply := GetReply{}
		DPrintf("[Clerk %d] Get %s", ck.clientId, key)
		ok := ck.servers[server].Call("KVServer.Get", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader {
			server = (server + 1) % len(ck.servers)
			continue
		}
		DPrintf("[Clerk %d] Get %s -> %s", ck.clientId, key, reply.Value)
		if reply.Err == ErrNoKey {
			return ""
		}
		if reply.Err == OK {
			ck.leaderId = server
			return reply.Value
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.requestId++
	requestId := ck.requestId
	server := ck.leaderId
	args := PutAppendArgs{Key: key, Value: value, Op: op, ClientId: ck.clientId, RequestId: requestId}
	for {
		reply := PutAppendReply{}
		DPrintf("[Clerk %d] %s %s %s", ck.clientId, op, key, value)
		ok := ck.servers[server].Call("KVServer.PutAppend", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader {
			server = (server + 1) % len(ck.servers)
			continue
		}
		DPrintf("[Clerk %d] %s %s %s -> %s", ck.clientId, op, key, value, reply.Err)
		if reply.Err == OK {
			ck.leaderId = server
			return
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

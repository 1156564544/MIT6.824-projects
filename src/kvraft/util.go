package kvraft

import "6.824/raft"

func (kv *KVServer) waitApplyCh() {
	for ch := range kv.applyCh {
		if ch.CommandValid == true {
			kv.AppliedOperation(ch)
		}
		if ch.SnapshotValid == true {
			kv.readSnapshot(ch)
		}
	}
}

func (kv *KVServer) AppliedOperation(ch raft.ApplyMsg) {
	op := ch.Command.(Op)
	if ch.CommandIndex <= kv.lastSSRaftIndex {
		return
	}
	if !kv.isRequestDuplicated(op.ClientId, op.RequestId) {
		if op.OpType == "Put" {
			kv.DoPut(op)
		}
		if op.OpType == "Append" {
			kv.DoAppend(op)
		}
	}

	if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() >= kv.maxraftstate*7/8 {
		kv.writeSnapshot(ch.CommandIndex)
		DPrintf("[%d] write snapshot at index %d", kv.me, ch.CommandIndex)
	}

	kv.SendChanneltoServer(op, ch.CommandIndex)
}

func (kv *KVServer) SendChanneltoServer(op Op, index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	toserverch, exist := kv.ch[index]
	if exist {
		toserverch <- op
	}
}

func (kv *KVServer) DoGet(op Op) (string, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	value, exist := kv.kvDB[op.Key]
	kv.lastrequestId[op.ClientId] = op.RequestId
	if !exist {
		return "", false
	}
	return value, true
}

func (kv *KVServer) DoPut(op Op) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.kvDB[op.Key] = op.Value
	kv.lastrequestId[op.ClientId] = op.RequestId
	return true
}

func (kv *KVServer) DoAppend(op Op) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	value, exist := kv.kvDB[op.Key]
	if !exist {
		kv.kvDB[op.Key] = op.Value
	} else {
		kv.kvDB[op.Key] = value + op.Value
	}
	kv.lastrequestId[op.ClientId] = op.RequestId
	return true
}

func (kv *KVServer) isRequestDuplicated(clientId int64, requestId int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	lastId, exist := kv.lastrequestId[clientId]
	if !exist {
		return false
	}
	return requestId <= lastId
}

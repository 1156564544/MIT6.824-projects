package kvraft

import (
	"6.824/labgob"
	"6.824/raft"
	"bytes"
)

func (kv *KVServer) writeSnapshot(raftIndex int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvDB)
	e.Encode(kv.lastrequestId)
	data := w.Bytes()
	kv.rf.Snapshot(raftIndex, data)
}

func (kv *KVServer) readSnapshot(message raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	data := message.Snapshot
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var kvDB map[string]string
	var lastrequestId map[int64]int
	if d.Decode(&kvDB) != nil || d.Decode(&lastrequestId) != nil {
		panic("read snapshot error")
	} else {
		kv.kvDB = kvDB
		kv.lastrequestId = lastrequestId
	}

	kv.lastSSRaftIndex = message.SnapshotIndex
}

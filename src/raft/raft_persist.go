package raft

import (
	"6.824/labgob"
	"bytes"
)

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persistData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastSSPointIndex)
	e.Encode(rf.lastSSPointTerm)
	data := w.Bytes()
	return data
}

func (rf *Raft) persist() {
	// Your code here (2C).
	data := rf.persistData()
	rf.persister.SaveRaftState(data)

}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var persist_currentTrem int
	var persist_voteFor int
	var persist_log []Entry
	var persist_lastSSpointIndex int
	var persist_lastSSpointTerm int
	if d.Decode(&persist_currentTrem) != nil ||
		d.Decode(&persist_voteFor) != nil ||
		d.Decode(&persist_log) != nil ||
		d.Decode(&persist_lastSSpointIndex) != nil ||
		d.Decode(&persist_lastSSpointTerm) != nil {
		DPrintf("[%d] readPersist error", rf.me)
	} else {
		rf.currentTerm = persist_currentTrem
		rf.votedFor = persist_voteFor
		rf.log = persist_log
		rf.lastSSPointIndex = persist_lastSSpointIndex
		rf.lastSSPointTerm = persist_lastSSpointTerm
	}
}

func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}

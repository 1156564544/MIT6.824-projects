package raft

import (
	"log"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func (rf *Raft) ChangeState(state int) {
	rf.status = state
	if state == Follower {
		//rf.heartbeatTime = time.Now()
		rf.votedFor = -1
		rf.votedNum = 0
		rf.persist()
	}
	if state == Candidate {
		rf.votedNum = 1
		rf.votedFor = rf.me
		rf.currentTerm += 1
		rf.persist()
		rf.heartbeatTime = time.Now()
	}
	if state == Leader {
		rf.votedFor = rf.me
		rf.persist()
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = rf.getLastLogIndex() + 1
			rf.matchIndex[i] = 0
		}
		DPrintf("Leader:%v, nextIndex:%v, matchIndex:%v", rf.me, rf.nextIndex, rf.matchIndex)
	}
}

//func (rf *Raft) getLastLogIndex() int {
//	return len(rf.log) - 1
//}
//
//func (rf *Raft) getLastLogTerm() int {
//	return rf.log[len(rf.log)-1].Term
//}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.log) - 1 + rf.lastSSPointIndex
}

func (rf *Raft) getLastLogTerm() int {
	if len(rf.log)-1 == 0 {
		return rf.lastSSPointTerm
	} else {
		return rf.log[len(rf.log)-1].Term
	}
}

func (rf *Raft) getLogWithIndex(globalIndex int) Entry {

	return rf.log[globalIndex-rf.lastSSPointIndex]
}

func (rf *Raft) getLogTermWithIndex(globalIndex int) int {
	if globalIndex-rf.lastSSPointIndex == 0 {
		return rf.lastSSPointTerm
	}
	return rf.log[globalIndex-rf.lastSSPointIndex].Term
}

func (rf *Raft) isUpToDate(index int, term int) bool {
	if term > rf.getLastLogTerm() {
		return true
	}
	if term == rf.getLastLogTerm() && index >= rf.getLastLogIndex() {
		return true
	}
	return false
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

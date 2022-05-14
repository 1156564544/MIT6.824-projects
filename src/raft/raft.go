package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const (
	Follower  int = 0
	Candidate int = 1
	Leader    int = 2

	MaxElectionTime  int = 300
	MinElectionTime  int = 100
	HeartbeatTimeout int = 100
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Entry struct {
	Command interface{}
	//Index   int
	Term int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	status    int
	votedNum  int

	currentTerm int
	votedFor    int
	log         []Entry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	heartbeatTime time.Time
	applyCh       chan ApplyMsg

	lastSSPointIndex int
	lastSSPointTerm  int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.status == Leader
	return term, isleader
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() == true {
		return -1, -1, false
	}
	if rf.status != Leader {
		return -1, -1, false
	} else {
		index := rf.getLastLogIndex() + 1
		//index := len(rf.log)
		term := rf.currentTerm
		rf.log = append(rf.log, Entry{Term: term, Command: command})
		DPrintf("[StartCommand] Leader %d get command %v,index %d", rf.me, command, index)
		rf.persist()
		return index, term, true
	}
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		sleepTime := rand.Intn(MaxElectionTime-MinElectionTime) + MinElectionTime
		time.Sleep(time.Duration(sleepTime) * time.Millisecond)
		rf.mu.Lock()
		if rf.heartbeatTime.Add(time.Duration(sleepTime)*time.Millisecond).Before(time.Now()) && rf.status != Leader {
			rf.ChangeState(Candidate)
			rf.mu.Unlock()
			rf.StartVote()
		} else {
			rf.mu.Unlock()
		}
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) appendEntriesTicker() {
	for rf.killed() == false {
		time.Sleep(time.Duration(HeartbeatTimeout) * time.Millisecond)
		rf.mu.Lock()
		if rf.status == Leader {
			rf.mu.Unlock()
			rf.BroadEntries()
		} else {
			rf.mu.Unlock()
		}

	}
}

func (rf *Raft) committedToAppliedTicker() {
	// put the committed entry to apply on the state machine
	for rf.killed() == false {
		time.Sleep(10 * time.Millisecond)
		rf.mu.Lock()

		if rf.lastApplied >= rf.commitIndex {
			rf.mu.Unlock()
			continue
		}

		Messages := make([]ApplyMsg, 0)
		// log.Printf("[!!!!!!--------!!!!!!!!-------]Restart, LastSSP: %d, LastApplied :%d, commitIndex %d",rf.lastSSPointIndex,rf.lastApplied,rf.commitIndex)
		//log.Printf("[ApplyEntry] LastApplied %d, commitIndex %d, lastSSPindex %d, len %d, lastIndex %d",rf.lastApplied,rf.commitIndex,rf.lastSSPointIndex, len(rf.log),rf.getLastIndex())
		for rf.lastApplied < rf.commitIndex && rf.lastApplied < rf.getLastLogIndex() {
			//for rf.lastApplied < rf.commitIndex {
			rf.lastApplied += 1
			//DPrintf("[ApplyEntry---] %d apply entry index %d, command %v, term %d, lastSSPindex %d",rf.me,rf.lastApplied,rf.getLogWithIndex(rf.lastApplied).Command,rf.getLogWithIndex(rf.lastApplied).Term,rf.lastSSPointIndex)
			Messages = append(Messages, ApplyMsg{
				CommandValid:  true,
				SnapshotValid: false,
				CommandIndex:  rf.lastApplied,
				Command:       rf.getLogWithIndex(rf.lastApplied).Command,
			})
		}
		rf.mu.Unlock()

		for _, messages := range Messages {
			rf.applyCh <- messages
		}
	}

}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.status = Follower
	rf.votedNum = 0
	rf.heartbeatTime = time.Now()
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]Entry, 1)
	rf.log[0] = Entry{Term: 0, Command: nil}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.applyCh = applyCh

	rf.lastSSPointIndex = 0
	rf.lastSSPointTerm = 0

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	if rf.lastSSPointIndex > 0 {
		rf.lastApplied = rf.lastSSPointIndex
	}
	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.appendEntriesTicker()

	go rf.committedToAppliedTicker()

	return rf
}

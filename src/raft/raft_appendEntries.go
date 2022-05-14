package raft

import (
	"time"
)

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term             int
	Success          bool
	ConflictingIndex int // optimizer func for find the nextIndex
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	term := args.Term
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 5.1
	if term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		DPrintf("Node:%v reject appendEntries from Node:%v, because term:%v < currentTerm:%v", rf.me, args.LeaderId, term, rf.currentTerm)
		return
	}
	rf.heartbeatTime = time.Now()
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.persist()
		rf.ChangeState(Follower)
	}
	// 5.3
	if rf.lastSSPointIndex > args.PrevLogIndex {
		reply.Success = false
		reply.ConflictingIndex = rf.getLastLogIndex() + 1
		return
	}

	if args.PrevLogIndex > rf.getLastLogIndex() {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.ConflictingIndex = rf.getLastLogIndex() + 1
		DPrintf("Node:%v reject appendEntries from Node:%v, because args.PrevLogIndex:%v >= len(rf.log):%v", rf.me, args.LeaderId, args.PrevLogIndex, rf.getLastLogIndex()+1)
		return
	}
	if rf.getLogTermWithIndex(args.PrevLogIndex) != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.ConflictingIndex = args.PrevLogIndex
		for i := args.PrevLogIndex; i >= 1; i-- {
			if rf.getLogTermWithIndex(i) != rf.getLogTermWithIndex(i-1) {
				reply.ConflictingIndex = i
				break
			}
		}
		DPrintf("Node:%v reject appendEntries from Node:%v, because rf.log[args.PrevLogIndex].Term(%v) != args.PrevLogTerm(%v)", rf.me, args.LeaderId, rf.getLogTermWithIndex(args.PrevLogIndex), args.PrevLogTerm)
		return
	}
	rf.log = rf.log[:args.PrevLogIndex+1-rf.lastSSPointIndex]
	rf.log = append(rf.log, args.Entries...)
	rf.persist()
	reply.Success = true
	reply.Term = rf.currentTerm
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIndex())
	}
	DPrintf("Node:%v accept appendEntries from Node:%v, log:%v", rf.me, args.LeaderId, rf.log)
	return
}

func (rf *Raft) sendBroadEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) BroadEntries() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(index int) {
			rf.mu.Lock()
			if rf.nextIndex[index] <= rf.lastSSPointIndex {
				rf.mu.Unlock()
				go rf.leaderSendSnapShot(index)
				DPrintf("Node:%v send snapshot to Node:%v", rf.me, index)
				return
			}

			request := AppendEntriesArgs{}
			reply := new(AppendEntriesReply)
			request.Term = rf.currentTerm
			request.LeaderId = rf.me
			request.PrevLogIndex = rf.nextIndex[index] - 1
			if request.PrevLogIndex > rf.getLastLogIndex() {
				request.PrevLogIndex = rf.getLastLogIndex()
			}
			request.PrevLogTerm = rf.getLogTermWithIndex(request.PrevLogIndex)
			request.LeaderCommit = rf.commitIndex
			if rf.nextIndex[index] <= rf.getLastLogIndex() {
				request.Entries = rf.log[rf.nextIndex[index]-rf.lastSSPointIndex:]
			} else {
				request.Entries = []Entry{}
			}
			rf.mu.Unlock()
			DPrintf("Node:%v append entries to node:%v, nextIndex:%v, matchIndex:%v, commitIndex:%v\n", rf.me, index, rf.nextIndex[index], rf.matchIndex[index], rf.commitIndex)
			ok := rf.sendBroadEntries(index, request, reply)
			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.status != Leader {
					return
				}
				DPrintf("Node:%v get response from node:%v, success:%v, term:%v\n", rf.me, index, reply.Success, reply.Term)
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.persist()
					rf.ChangeState(Follower)
					return
				}
				if reply.Success {
					rf.matchIndex[index] = min(request.PrevLogIndex+len(request.Entries), rf.getLastLogIndex())
					rf.nextIndex[index] = rf.matchIndex[index] + 1
					for i := rf.matchIndex[index]; i > rf.commitIndex; i-- {
						// Figure 8
						if rf.getLogTermWithIndex(i) != rf.currentTerm {
							break
						}
						num := 1
						for j := 0; j < len(rf.peers); j++ {
							if j == rf.me || rf.matchIndex[j] < i {
								continue
							}
							if rf.matchIndex[j] >= i {
								num += 1
							}
						}
						if num > len(rf.peers)/2 {
							rf.commitIndex = i
							DPrintf("Node:%v commitIndex:%v\n", rf.me, rf.commitIndex)
							break
						}
					}

				} else {
					rf.nextIndex[index] = reply.ConflictingIndex
					if rf.nextIndex[index] == 0 {
						rf.nextIndex[index] = 1
					}
				}
			}
		}(i)
	}
}

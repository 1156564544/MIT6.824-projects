package raft

import "time"

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	DPrintf("Node:%v with term %v and log:%v receive RequestVote %v", rf.me, rf.currentTerm, rf.log, args)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := args.Term
	candidateId := args.CandidateId
	if term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		DPrintf("Node:%v with term %v reject RequestVote %v", rf.me, rf.currentTerm, args)
		return
	}
	if term > rf.currentTerm {
		rf.ChangeState(Follower)
		rf.currentTerm = term
		rf.persist()
	}
	if (rf.votedFor == -1 || rf.votedFor == candidateId) && rf.isUpToDate(args.LastLogIndex, args.LastLogTerm) {
		rf.votedFor = candidateId
		rf.persist()
		rf.heartbeatTime = time.Now()
		reply.VoteGranted = true
		reply.Term = term
		DPrintf("Node:%v with term %v accept vote for %v", rf.me, rf.currentTerm, candidateId)
	} else {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		DPrintf("Node:%v with term %v reject vote for %v", rf.me, rf.currentTerm, candidateId)
	}
	return
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) StartVote() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(index int) {
			rf.mu.Lock()
			args := RequestVoteArgs{}
			reply := new(RequestVoteReply)
			args.Term = rf.currentTerm
			args.CandidateId = rf.me
			args.LastLogTerm = rf.getLastLogTerm()
			args.LastLogIndex = rf.getLastLogIndex()
			rf.mu.Unlock()
			DPrintf("Node: %d, start vote to node: %d, term: %d, last log term: %d, last log index: %d", rf.me, index, args.Term, args.LastLogTerm, args.LastLogIndex)
			ok := rf.sendRequestVote(index, args, reply)
			if ok {
				DPrintf("Node:%v receive vote reply from node:%v, term:%v, vote granted:%v", rf.me, index, reply.Term, reply.VoteGranted)
				rf.mu.Lock()
				if reply.Term < rf.currentTerm || rf.status != Candidate {
					rf.mu.Unlock()
					return
				}

				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.persist()
					rf.ChangeState(Follower)
					rf.mu.Unlock()
					return
				}

				if rf.status == Candidate && reply.VoteGranted {
					rf.votedNum += 1
					if rf.votedNum > (len(rf.peers)-1)/2 {
						rf.ChangeState(Leader)
						rf.mu.Unlock()
						rf.BroadEntries()
						return
					}
				}
				rf.mu.Unlock()
			}
		}(i)
	}
}

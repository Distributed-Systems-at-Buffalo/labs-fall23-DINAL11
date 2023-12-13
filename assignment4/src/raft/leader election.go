package raft

func (rf *Raft) getLastIndex() int {
	return len(rf.log) - 1
}
func (rf *Raft) getLastTerm() int {
	return rf.log[len(rf.log)-1].LogTerm
}

func (rf *Raft) boatcastRequestVote() {
	var args RequestVoteArgs
	rf.mu.Lock()

	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogTerm = rf.getLastTerm()
	args.LastLogIndex = rf.getLastIndex()
	rf.mu.Unlock()

	for i := range rf.peers {
		if i != rf.me && rf.state == STATE_CANDIDATE {
			go func(i int) {
				var reply RequestVoteReply
				rf.sendRequestVote(i, args, &reply)
			}(i)
		}
	}
}

func (rf *Raft) boatcastAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	N := rf.commitIndex
	last := rf.getLastIndex()
	for i := rf.commitIndex + 1; i <= last; i++ {
		num := 1
		for j := range rf.peers {
			if j != rf.me && rf.matchIndex[j] >= i && rf.log[i].LogTerm == rf.currentTerm {
				num++
			}
		}
		if 2*num > len(rf.peers) {
			N = i
		}
	}
	if N != rf.commitIndex {
		rf.commitIndex = N
		rf.commitCh <- true
	}

	for i := range rf.peers {
		if i != rf.me && rf.state == STATE_LEADER {
			var args AppendEntriesArgs
			args.Term = rf.currentTerm
			args.LeaderId = rf.me
			args.PrevLogIndex = rf.nextIndex[i] - 1
			args.PrevLogTerm = rf.log[args.PrevLogIndex].LogTerm
			args.Entries = make([]LogEntry, len(rf.log[args.PrevLogIndex+1:]))
			copy(args.Entries, rf.log[args.PrevLogIndex+1:])

			args.LeaderCommit = rf.commitIndex
			go func(i int, args AppendEntriesArgs) {
				var reply AppendEntriesReply
				rf.sendAppendEntries(i, args, &reply)
			}(i, args)
		}
	}
}

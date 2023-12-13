package raft

//
//type LogEntry struct {
//	Command interface{}
//	Term    int
//	Index   int
//}
//
//type AppendEntriesArgs struct {
//	Term     int
//	LeaderId int
//
//	PrevLogIndex int
//	PrevLogTerm  int
//	Entries      []LogEntry
//	LeaderCommit int
//}
//
//type AppendEntriesReply struct {
//	Term      int
//	Success   bool
//	NextTrail int
//}
//
//func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
//
//	rf.mu.Lock()
//	defer rf.mu.Unlock()
//	reply.Term = rf.currentTerm
//
//	if args.Term < rf.currentTerm {
//		reply.Success = false
//		return
//	}
//
//	rf.heartbeatSignal <- true
//
//	if rf.serverstate != Leaders || args.Term != rf.currentTerm {
//		return
//	} else if reply.Term > rf.currentTerm {
//		rf.updateToFollower(reply.Term)
//	}
//
//	reply.Success = true
//
//	//rf.heartbeatTimer.Reset(heartbeatInterval)
//}
//
//func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
//	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
//
//	rf.mu.Lock()
//	defer rf.mu.Unlock()
//
//	//time.Sleep(heartbeatInterval)
//	///adding new lines to check the commit on Github
//	return ok
//}
//
//func (rf *Raft) updateToFollower(newTerm int) {
//	//rf.mu.Lock()
//	//defer rf.mu.Unlock()
//	println("updating to follower")
//	rf.currentTerm = newTerm
//	//rf.votedFor = 0
//	rf.votedFor = -1
//
//	rf.serverstate = Followers
//	return
//}

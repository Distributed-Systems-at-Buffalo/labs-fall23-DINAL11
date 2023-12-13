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
//   ask a Raft for its currenT term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

const (
	Followers = 0
	// a follower increments its current term and transitions to candidate state
	Candidates = Followers + 1
	Leaders    = Candidates + 1
)

const (
	heartbeatInterval = 50 * time.Millisecond
	electionTimeout   = 500 * time.Millisecond
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//Persistent state on all servers:
	currentTerm int
	votedFor    int
	log         []LogEntry

	//Volatile state on all servers:
	commitIndex int
	lastApplied int

	//Volatile state on leaders:
	nextIndex  []int
	matchIndex []int

	//stopCh chan struct{}

	serverstate     int
	receivedvote    int
	voteSignal      chan struct{}
	heartbeatTimer  *time.Timer
	heartbeatSignal chan interface{}
	uptodate        bool
	leaderSignal    chan interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.

	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	term = rf.currentTerm
	if rf.serverstate == Leaders {
		isleader = true
	}

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}

// example RequestVote RPC arguments structure.
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	//Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	//1. Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}

	// Check if candidate's log is at least as up-to-date as receiver's log (§5.2, §5.4)
	//candidateUpToDate := args.LastLogTerm > rf.getLastLogTerm() || (args.LastLogTerm == rf.getLastLogTerm() && args.LastLogIndex >= rf.getLastLogIndex())

	//2. If votedFor is null or candidateId, and candidate’s log is at
	//least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	if args.Term > rf.currentTerm /*|| rf.votedFor == -1 || rf.votedFor == args.CandidateId /*&& candidateUpToDate*/ {
		rf.currentTerm = args.Term
		rf.serverstate = Followers
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	}

}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		//
		//	//term := rf.currentTerm
		if rf.serverstate != Candidates || args.Term != rf.currentTerm {
			return ok
		}

		//
		//if !rf.shouldProcessVoteRequest(args) {
		//	return ok
		//}

		if reply.Term > rf.currentTerm {
			rf.updateToFollower2(reply.Term)
			//rf.currentTerm = reply.Term
			//rf.serverstate = Followers
			//rf.votedFor = -1
			//return ok
		}

		return reply.VoteGranted
	}

	return ok

}

// // isCandidate checks if the server is in the Candidates state.
func (rf *Raft) isCandidate() bool {
	return rf.serverstate == Candidates
}

// shouldProcessVoteRequest checks if the vote request should be processed based on the server state and term.
func (rf *Raft) shouldProcessVoteRequest(args RequestVoteArgs) bool {
	return rf.isCandidate() || args.Term == rf.currentTerm
}

// updateToFollower updates the server state to Follower and resets votedFor.
func (rf *Raft) updateToFollower2(newTerm int) {
	rf.currentTerm = newTerm
	rf.serverstate = Followers
	rf.votedFor = -1
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	return index, term, isLeader
}

// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	// Your code here, if desired.
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

	//println("Make")
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.

	// Concept: Everything is first initiated as a Follower
	rf.serverstate = Followers // the serverstate is Follower
	//rf.votedFor = 0
	rf.votedFor = -1   // there are no votes given at this point, since the Followers, Candidates and Leader assigned, 0, 1, 2... -1 is used
	rf.currentTerm = 0 // the current Term, initial election term is Zero

	// Make Channels to pass on signals for Heartbeart and Leader existence
	rf.heartbeatSignal = make(chan interface{})
	rf.leaderSignal = make(chan interface{})

	// Make Log Entries to store
	//rf.uptodate = false
	//rf.log = make([]LogEntry, 0)

	rf.heartbeatTimer = time.NewTimer(heartbeatInterval)
	//go rf.heartbeatSender()

	// Initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// State machine loop
	go rf.statetransitions()

	return rf
}

// goroutine that periodically sends heartbeats to other nodes.
//func (rf *Raft) heartbeatSender() {
//	for {
//		<-rf.heartbeatTimer.C
//		rf.sendHeartbeat()
//		//println("heartbeat timer activated")
//		rf.heartbeatTimer.Reset(heartbeatInterval)
//	}
//	//ticker := time.Tick(50 * time.Millisecond)
//	//
//	//for range ticker {
//	//	rf.sendHeartbeat()
//	//}
//}

// sends heartbeats to other nodes when the Raft node is in the leader state.
func (rf *Raft) sendHeartbeat() {
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	leaderID := rf.me
	rf.mu.Unlock()

	//Send heartbeats to all peers
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue // Skip sending heartbeat to itself
		}

		//if rf.serverstate == Leaders {
		//	go rf.sendappendentriestoserver(i, currentTerm, leaderID)
		//}
		go rf.sendappendentriestoserver(i, currentTerm, leaderID)
	}
	time.Sleep(heartbeatInterval)
}

// sends AppendEntries RPCs to a specific server if the Raft node is in the leader state
func (rf *Raft) sendappendentriestoserver(server, currentTerm, leaderID int) {
	args := AppendEntriesArgs{
		Term:     currentTerm,
		LeaderId: leaderID,
	}

	var reply AppendEntriesReply

	// Send AppendEntries RPC to each follower
	if rf.serverstate == Leaders {
		rf.sendAppendEntries(server, args, &reply)
	}
}

func (rf *Raft) statetransitions() {
	//println("state transition")
	for {
		select {
		case <-rf.heartbeatSignal:
			// Received a heartbeat signal, continue with the current state
		case <-time.After(electionTimeout):
		}
		switch rf.serverstate {
		case Followers:
			//println("follower loop Activated")
			rf.followerLoop()

		case Leaders:
			//println("Leader loop Activated")
			rf.leaderLoop()

		case Candidates:
			//println("Candidate loop Activated")
			rf.candidateLoop()
		}
	}
}

func (rf *Raft) followerLoop() {
	// Calculate timeout duration at the start, Randomize this value over each Follower
	ElecTO := time.Duration(rand.Int63()%150+150) * time.Millisecond
	timer := time.NewTimer(ElecTO)

	select {
	//If there's a heartbeat signal is existing, skip any actions
	case <-rf.heartbeatSignal:
		// Received heartbeat, stay in follower state
		//timer.Reset()
		//timer.Stop()
	// Change Follower to Candidate term when no Heartbeat received and the ElecTO is over
	case <-timer.C:
		//rf.mu.Lock()
		rf.serverstate = Candidates
		//rf.mu.Unlock()
	}

}

func (rf *Raft) leaderLoop() {
	rf.sendHeartbeat()
	//time.Sleep(heartbeatInterval)
}

func (rf *Raft) candidateLoop() {
	// Candidate prepares for the election
	rf.startElection()
}

//package raft

type LogEntry struct {
	Command interface{}
	Term    int
	Index   int
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term      int
	Success   bool
	NextTrail int
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	rf.heartbeatSignal <- true

	if rf.serverstate != Leaders || args.Term != rf.currentTerm {
		return
	} else if reply.Term > rf.currentTerm {
		rf.updateToFollower(reply.Term)
	}

	reply.Success = true

	//rf.heartbeatTimer.Reset(heartbeatInterval)
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	//time.Sleep(heartbeatInterval)
	///adding new lines to check the commit on Github
	return ok
}

func (rf *Raft) updateToFollower(newTerm int) {
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	println("updating to follower")
	rf.currentTerm = newTerm
	//rf.votedFor = 0
	rf.votedFor = -1

	rf.serverstate = Followers
	return
}

//package raft

//import (
//"math/rand"
//"time"
//)

func (rf *Raft) getLastLogIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) getLastLogTerm() int {
	lastIndex := rf.getLastLogIndex()
	if lastIndex >= 0 {
		return rf.log[lastIndex].Term
	}
	return 0
}

func (rf *Raft) initialelection() {
	//pass fail pass fail
	//rf.mu.Lock()

	// Increment current term and set votedFor to self.
	//rf.currentTerm++
	//rf.votedFor = rf.me

	// Store current state for sending votes.
	//rf.receivedvote = 1
	currentTerm := rf.currentTerm
	candidateID := rf.me
	//rf.mu.Unlock()

	lastLogIndex := rf.getLastLogIndex()
	lastLogTerm := rf.getLastLogTerm()

	//to collect vote results
	voteResults := make(chan bool, len(rf.peers)-1)

	for peerindex := range rf.peers {
		if peerindex != rf.me {
			go func(peerIndex int) {
				args := RequestVoteArgs{
					Term:         currentTerm,
					CandidateId:  candidateID,
					LastLogIndex: lastLogIndex,
					LastLogTerm:  lastLogTerm,
				}
				var reply RequestVoteReply
				voteResults <- rf.sendRequestVote(peerIndex, args, &reply)

				if <-voteResults {
					rf.receivedvote++
					if rf.receivedvote > len(rf.peers)/2 {
						rf.serverstate = Leaders
						rf.leaderSignal <- true
						//break
					}
				}

			}(peerindex)
		}
	}
	// Track received votes and become leader if enough votes are received.
	//rf.receivedvote = 1
	//for rV := range rf.peers {
	//	if rV != rf.me{
	//
	//	}
	//	//for range rf.peers[1:] {
	//	if <-voteResults {
	//		rf.receivedvote++
	//		if rf.receivedvote > len(rf.peers)/2 {
	//			rf.serverstate = Leaders
	//			rf.leaderSignal <- true
	//			//break
	//		}
	//	}
	//}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	//if rf.serverstate != Leaders {
	//	rf.serverstate = Followers
	//}

}

// candidateLoop represents the behavior of a Raft candidate.
func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.receivedvote = 1
	rf.mu.Unlock()

	go rf.initialelection()

	ElecTO2 := time.Duration(rand.Int63()%150+150) * time.Millisecond
	electionTimer := time.NewTimer(ElecTO2)

	select {
	// Timeout or receive heartbeat other nodes become leader first to become a follower
	case <-electionTimer.C:
		rf.mu.Lock()
		rf.serverstate = Followers
		rf.mu.Unlock()

	case <-rf.heartbeatSignal:
		rf.mu.Lock()
		rf.serverstate = Followers
		rf.mu.Unlock()

	// Receive a message from leaderCh channel indicating winning the election, become a leader
	case <-rf.leaderSignal:
		rf.mu.Lock()
		rf.serverstate = Leaders
		rf.mu.Unlock()
	}

	electionTimer.Stop()
}

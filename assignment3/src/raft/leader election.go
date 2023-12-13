package raft

import (
	"math/rand"
	"time"
)

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

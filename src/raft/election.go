package raft

import (
	"math/rand"
	"time"
)

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// RequestVote RPC Handler
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {

	rf.stateCond.L.Lock()
	if args.Term < *rf.CurrentTerm {
		logger.Debugf("%s[%d] received a RequestVote from Candidate[%d] in Term[%d]. Send newer Term[%d] to force it step down.",
			rf.state, rf.me, args.CandidateId, *rf.CurrentTerm, args.Term)
		reply.Term = *rf.CurrentTerm
		reply.VoteGranted = false
	} else {
		if args.Term > *rf.CurrentTerm {
			logger.Debugf("Stale %s[%d] received a RequestVote from Candidate[%d] in Term[%d] with newer Term[%d]. Step down.",
				rf.state, rf.me, args.CandidateId, *rf.CurrentTerm, args.Term)
			*rf.CurrentTerm = args.Term
			rf.VotedFor = nil
			rf.toState(Follower)
		}
		reply.Term = *rf.CurrentTerm
		if rf.VotedFor == nil {
			lastIndex := rf.lastNewEntryIndex
			//implement Election restriction
			if lastIndex < 1 || args.LastLogTerm > rf.getEntry(lastIndex).Term || (args.LastLogTerm == rf.getEntry(lastIndex).Term && args.LastLogIndex >= lastIndex) {
				logger.Debugf("%s[%d] grant a vote for the Candidate[%d], since it has a more up-to-date logs.",
					rf.state, rf.me, args.CandidateId)
				rf.VotedFor = new(int)
				*rf.VotedFor = args.CandidateId
				reply.VoteGranted = true
				rf.leaderHeartBeat = true
			} else {
				logger.Debugf("%s[%d] declined RequestVote from the Candidate[%d], since it doesn't have a more up-to-date logs.",
					rf.state, rf.me, args.CandidateId)
				reply.VoteGranted = false
			}
		} else {
			logger.Debugf("%s[%d] declined a RequestVote from Candidate[%d] in Term[%d], since it already grant vote to Candidate[%d]",
				rf.state, rf.me, args.CandidateId, *rf.CurrentTerm, *rf.VotedFor)
			reply.VoteGranted = false
			rf.leaderHeartBeat = true
		}
	}
	rf.stateCond.L.Unlock()
	return nil
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers.GetEnd(server).Call("Raft.RequestVote", args, reply)
	rf.stateCond.L.Lock()

	if ok {
		if reply.Term > *rf.CurrentTerm {
			logger.Debugf("%s[%d] received a RequestVoteReply in Term[%d] with newer Term[%d] from server[%d]. Step down",
				rf.state, rf.me, *rf.CurrentTerm, reply.Term, server)
			*rf.CurrentTerm = reply.Term
			rf.toState(Follower)
		} else if rf.state == Candidate && reply.Term == *rf.CurrentTerm {
			if reply.VoteGranted {
				rf.voteCount++
				logger.Debugf("%s[%d] received a RequestVoteReply from server[%d]. It grant vote(%d/%d votes).",
					rf.state, rf.me, server, rf.voteCount, rf.peers.Len())
				if rf.voteCount > rf.peers.Len()/2 {
					logger.Debugf("%s[%d] become Leader in Term[%d]", rf.state, rf.me, *rf.CurrentTerm)
					rf.toState(Leader)
					rf.initLeader()
				}
			} else {
				logger.Debugf("%s[%d] received a RequestVoteReply from server[%d]. It decline vote.",
					rf.state, rf.me, server)
				rf.declined = true
			}
		}
	}
	rf.stateCond.L.Unlock()
	return ok
}

func (rf *Raft) initiateElection() {
	logger.Debugf("%s[%d] election timeout, convert to new Term[%d] and initiate a Election.",
		rf.state, rf.me, *rf.CurrentTerm+1)
	*rf.CurrentTerm++
	rf.VotedFor = new(int)
	*rf.VotedFor = rf.me
	rf.voteCount = 1
	rf.declined = false
	rf.toState(Candidate)
	for i := 0; i < rf.peers.Len(); i++ {
		if i != rf.me {
			args := new(RequestVoteArgs)
			args.Term = *rf.CurrentTerm
			args.CandidateId = rf.me
			args.LastLogIndex = rf.lastNewEntryIndex
			if args.LastLogIndex < 0 {
				args.LastLogTerm = -1
			} else {
				args.LastLogTerm = rf.getEntry(args.LastLogIndex).Term
			}
			reply := new(RequestVoteReply)
			go rf.sendRequestVote(i, args, reply)
		}
	}
}

func (rf *Raft) electionTimeout() {
	for {
		rf.stateCond.L.Lock()
		for rf.state == Leader {
			rf.stateCond.Wait()
		}
		if rf.state == Dead {
			break
		}
		if rf.leaderHeartBeat {
			rf.leaderHeartBeat = false
		} else {
			rf.initiateElection()
		}
		interval := rand.Intn(ElectionTimeoutMax-ElectionTimeoutMin) + ElectionTimeoutMin
		if rf.declined {
			interval = ElectionTimeoutMax
		} else {
			interval = rand.Intn(ElectionTimeoutMax-ElectionTimeoutMin) + ElectionTimeoutMin
		}
		rf.declined = false
		rf.stateCond.L.Unlock()

		time.Sleep(time.Duration(interval) * time.Millisecond)
	}
}

package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isLeader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"sync"
	"time"
	"math/rand"
	"bytes"
	"encoding/gob"
	"rpc"
	"data"
	"github.com/op/go-logging"
)

var logger = logging.MustGetLogger("raft")

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool
	Snapshot    []byte
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex     // Lock to protect shared access to this peer's state
	peers     rpc.Peers      // RPC end points of all peers
	persister data.Persister // Object to hold this peer's persisted state
	me        int            // this peer's index into peers[]
	applyCh   chan ApplyMsg

	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//Persistent state on all servers
	CurrentTerm *int
	VotedFor    *int
	Log         *[]Entry

	//Volatile state on all servers
	commitIndex       int
	lastApplied       int
	lastNewEntryIndex int
	lastSnapshotIndex int
	lastSnapshotTerm  int

	//Volatile state on leaders
	nextIndex  map[int]int
	matchIndex map[int]int

	//Current state
	state     State
	voteCount int

	leaderHeartBeat bool
	declined        bool

	stateCond      *sync.Cond
	commitCond     *sync.Cond
	logReplicaChan *[] chan bool
	replicaEnd     sync.WaitGroup

	snapshotBuffer    []byte
	snapshotApplyChan chan []byte
	snapshotChan      chan bool

	onLeaderInitCallback      func()
	onLeaderEndCallback       func()
	onSnapshotCallback        func() interface{}
	onInstallSnapshotCallback func(d *gob.Decoder)
}

const (
	HeartBeatInterval  = 110
	ElectionTimeoutMin = 150
	ElectionTimeoutMax = 300
	SnapshotChunkSize  = 500000
)

type State string

const (
	Leader    State = "Leader"
	Candidate State = "Candidate"
	Follower  State = "Follower"
	Dead      State = "Dead"
)

// The unit of command, which will be persisted by log.
type Entry struct {
	Command interface{}
	Term    int
	Index   int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isLeader bool
	rf.stateCond.L.Lock()
	isLeader = rf.state == Leader
	term = *rf.CurrentTerm
	rf.stateCond.L.Unlock()
	return term, isLeader
}

func (rf *Raft) toState(state State) {
	oldState := rf.state
	rf.state = state
	if oldState == Leader && rf.state != Leader {
		rf.endLeader()
	}
	if oldState != rf.state {
		rf.stateCond.Broadcast()
	}
}

func (rf *Raft) OnLeaderInit(callback func()) {
	rf.onLeaderInitCallback = callback
}

func (rf *Raft) OnLeaderEnd(callback func()) {
	rf.onLeaderEndCallback = callback
}

func (rf *Raft) OnSnapshot(callback func() interface{}) {
	rf.onSnapshotCallback = callback
}
func (rf *Raft) GetTermByIndex(index int) int {
	rf.stateCond.L.Lock()
	defer rf.stateCond.L.Unlock()

	if index <= rf.lastNewEntryIndex {
		return rf.getEntry(index).Term
	} else {
		return 0
	}
}

func (rf *Raft) getEntry(index int) Entry {
	return (*rf.Log)[index-rf.lastSnapshotIndex]
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	if rf.VotedFor == nil {
		rf.VotedFor = new(int)
		*rf.VotedFor = -1
	}
	e.Encode(rf.VotedFor)
	if *rf.VotedFor == -1 {
		rf.VotedFor = nil
	}
	*rf.Log = (*rf.Log)[:rf.lastNewEntryIndex+1-rf.lastSnapshotIndex]
	e.Encode(rf.Log)
	content := w.Bytes()
	rf.persister.SaveState(content)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(persister data.Persister) {
	// Example:

	snapshot := persister.ReadSnapshot()
	if snapshot != nil {
		rf.loadSnapshot(snapshot)
	} else {
		rf.lastSnapshotIndex = 0
		rf.lastSnapshotTerm = 0
		rf.lastNewEntryIndex = 0
		rf.commitIndex = 0
		rf.lastApplied = 0
	}

	raftState := persister.ReadState()
	if raftState != nil {
		logger.Debugf("%s[%d] recover from persistence.", rf.state, rf.me)
		r := bytes.NewBuffer(raftState)
		d := gob.NewDecoder(r)
		d.Decode(&rf.CurrentTerm)
		d.Decode(&rf.VotedFor)
		if *rf.VotedFor == -1 {
			rf.VotedFor = nil
		}
		d.Decode(&rf.Log)
		rf.lastNewEntryIndex = len(*rf.Log) + rf.lastSnapshotIndex - 1
	} else {
		logger.Debugf("%s[%d] initialize.", rf.state, rf.me)
		rf.CurrentTerm = new(int)
		*rf.CurrentTerm = 0
		rf.VotedFor = nil
		rf.Log = new([]Entry)
		*rf.Log = append(*rf.Log, Entry{Term: rf.lastSnapshotTerm, Index: rf.lastSnapshotIndex})
	}

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
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

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term         int
	Success      bool
	Inconsistent bool
	ConflictTerm int
	FirstIndex   int
}

// AppendEntries RPC Handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {

	rf.stateCond.L.Lock()
	reply.Inconsistent = false
	if args.Term < *rf.CurrentTerm {
		logger.Debugf("%s[%d] received a AppendEntries from Stale Leader[%d] in Term[%d]. Send newer Term[%d] to force it step down.",
			rf.state, rf.me, args.LeaderId, args.Term, *rf.CurrentTerm)
		reply.Term = *rf.CurrentTerm
		reply.Success = false
	} else {
		rf.leaderHeartBeat = true
		if args.Term > *rf.CurrentTerm {
			logger.Debugf("Stale %s[%d] received a AppendEntries from Leader[%d] in Term[%d] with newer Term[%d]. Step down.",
				rf.state, rf.me, args.LeaderId, *rf.CurrentTerm, args.Term)
			*rf.CurrentTerm = args.Term
			rf.VotedFor = nil
			rf.toState(Follower)
		} else if rf.state == Candidate {
			logger.Debugf("Stale %s[%d] received a AppendEntries from Leader[%d] in Term[%d]. Step down.",
				rf.state, rf.me, args.LeaderId, *rf.CurrentTerm, args.Term)
			rf.toState(Follower)
		}
		reply.Term = *rf.CurrentTerm
		if rf.lastNewEntryIndex < args.PrevLogIndex || (args.PrevLogIndex >= rf.lastApplied && rf.getEntry(args.PrevLogIndex).Term != args.PrevLogTerm ) {
			reply.Success = false
			reply.Inconsistent = true
			if rf.lastNewEntryIndex < args.PrevLogIndex {
				reply.FirstIndex = rf.lastNewEntryIndex
				reply.ConflictTerm = rf.getEntry(reply.FirstIndex).Term
			} else {
				reply.ConflictTerm = rf.getEntry(args.PrevLogIndex).Term
				reply.FirstIndex = args.PrevLogIndex
				for reply.FirstIndex > rf.lastSnapshotIndex && rf.getEntry(reply.FirstIndex - 1).Term == reply.ConflictTerm {
					reply.FirstIndex -= 1
				}
			}

			logger.Debugf("%s[%d] declined a AppendEntries from Leader[%d] since the Entry at PrevLogIndex doesn't match.",
				rf.state, rf.me, args.LeaderId)
		} else {
			reply.Success = true
			modifyFlag := args.PrevLogIndex+len(args.Entries) > rf.lastNewEntryIndex

			for i, entry := range args.Entries {
				if args.PrevLogIndex+i+1 <= rf.lastApplied {
					continue
				}
				f := rf.appendEntry(args.PrevLogIndex+i+1, entry)
				modifyFlag = modifyFlag || f
			}
			if modifyFlag {
				rf.lastNewEntryIndex = args.PrevLogIndex + len(args.Entries)
				rf.persist()
			}
			logger.Debugf("%s[%d] received a AppendEntries[len=%d] from Leader[%d] and update lastNewEntryIndex[%d]",
				rf.state, rf.me, len(args.Entries), args.LeaderId, rf.lastNewEntryIndex)
			if args.LeaderCommit > rf.commitIndex {
				if args.LeaderCommit > rf.lastNewEntryIndex {
					rf.commitIndex = rf.lastNewEntryIndex
				} else {
					rf.commitIndex = args.LeaderCommit
				}
				logger.Debugf("%s[%d] update new commitIndex[%d]", rf.state, rf.me, rf.commitIndex)
				rf.commitCond.Broadcast()
			}
		}
	}
	rf.stateCond.L.Unlock()
	return nil
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers.GetEnd(server).Call("Raft.AppendEntries", args, reply)
	rf.stateCond.L.Lock()
	if ok && rf.state == Leader {
		if reply.Term > *rf.CurrentTerm {
			logger.Debugf("%s[%d] received a AppendEntriesReply in Term[%d] with newer Term[%d] from server[%d]. Step down.",
				rf.state, rf.me, *rf.CurrentTerm, reply.Term, server)
			*rf.CurrentTerm = reply.Term
			rf.toState(Follower)
			rf.leaderHeartBeat = true
		} else if reply.Term == *rf.CurrentTerm {
			if reply.Success {
				if rf.nextIndex[server] < args.PrevLogIndex+len(args.Entries)+1 {
					rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
				}
				if rf.matchIndex[server] < args.PrevLogIndex+len(args.Entries) {
					rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
					rf.updateCommitIndex()
					rf.commitCond.Broadcast()
				}
				logger.Debugf("%s[%d] AppendEntries to server[%d] success. Update nextIndex[%d] and matchIndex[%d]",
					rf.state, rf.me, server, rf.nextIndex[server], rf.matchIndex[server])

			} else if reply.Inconsistent {
				rf.nextIndex[server] = reply.FirstIndex
				if reply.FirstIndex == 0 {
					rf.nextIndex[server] = 1
				}
				logger.Debugf("%s[%d] AppendEntries to server[%d] fail. Decrement nextIndex[%d] and try again. ",
					rf.state, rf.me, server, rf.nextIndex[server])
				select {
				case (*rf.logReplicaChan)[server] <- true:
					break
				default:
				}
			}
		}
	}
	rf.stateCond.L.Unlock()
	return ok
}

type Snapshot struct {
	LastIncludedIndex int
	LastIncludedTerm  int
	State             interface{}
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) error {
	rf.stateCond.L.Lock()
	if args.Term < *rf.CurrentTerm {
		logger.Debugf("%s[%d] received a InstallSnapshot from Stale Leader[%d] in Term[%d]. Send newer Term[%d] to force it step down.",
			rf.state, rf.me, args.LeaderId, args.Term, *rf.CurrentTerm)
		reply.Term = *rf.CurrentTerm
	} else {
		rf.leaderHeartBeat = true
		if args.Term > *rf.CurrentTerm {
			logger.Debugf("Stale %s[%d] received a InstallSnapshot from Leader[%d] in Term[%d] with newer Term[%d]. Step down.",
				rf.state, rf.me, args.LeaderId, *rf.CurrentTerm, args.Term)
			*rf.CurrentTerm = args.Term
			rf.VotedFor = nil
			rf.toState(Follower)
		} else if rf.state == Candidate {
			logger.Debugf("Stale %s[%d] received a InstallSnapshot from Leader[%d] in Term[%d]. Step down.",
				rf.state, rf.me, args.LeaderId, *rf.CurrentTerm, args.Term)
			rf.toState(Follower)
		}

		logger.Debugf("%s[%d] received a snapshot chunk from Leader[%d] at offset[%d].",
			rf.state, rf.me, args.LeaderId, args.Offset)
		if args.Offset == 0 {
			rf.snapshotBuffer = make([]byte, 0)
		}
		for len(rf.snapshotBuffer) < args.Offset {
			rf.snapshotBuffer = append(rf.snapshotBuffer, byte(0))
		}
		for i, e := range args.Data {
			if len(rf.snapshotBuffer) > i+args.Offset {
				rf.snapshotBuffer[i+args.Offset] = e
			} else {
				rf.snapshotBuffer = append(rf.snapshotBuffer, e)
			}
		}
		if args.Done {
			logger.Debugf("%s[%d] has received all snapshot chunks, persist snapshot, apply it and discard covered logs.",
				rf.state, rf.me)
			rf.persister.SaveSnapshot(rf.snapshotBuffer)

			rf.loadSnapshot(rf.persister.ReadSnapshot())
			rf.persist()
			go func() {
				rf.snapshotApplyChan <- rf.persister.ReadSnapshot()
			}()
			rf.snapshotBuffer = nil
		}
	}
	rf.stateCond.L.Unlock()
	return nil
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	snapshot := args.Data
	for offset := 0; offset < len(snapshot); offset += SnapshotChunkSize {
		args.Offset = offset
		if offset+SnapshotChunkSize >= len(snapshot) {
			args.Data = snapshot[offset:]
			args.Done = true
		} else {
			args.Data = snapshot[offset:offset+SnapshotChunkSize]
			args.Done = false
		}
		reply = new(InstallSnapshotReply)
		for !rf.peers.GetEnd(server).Call("Raft.InstallSnapshot", args, reply) {
			rf.stateCond.L.Lock()
			if rf.state != Leader {
				rf.stateCond.L.Unlock()
				return false
			}
			reply = new(InstallSnapshotReply)
			rf.stateCond.L.Unlock()
		}
		rf.stateCond.L.Lock()
		if rf.state == Leader {
			if reply.Term > *rf.CurrentTerm {
				logger.Debugf("%s[%d] received a InstallSnapshotReply in Term[%d] with newer Term[%d] from server[%d]. Step down.",
					rf.state, rf.me, *rf.CurrentTerm, reply.Term, server)
				*rf.CurrentTerm = reply.Term
				rf.toState(Follower)
				rf.leaderHeartBeat = true
			} else if args.Done {
				rf.nextIndex[server] = args.LastIncludedIndex + 1
				if rf.matchIndex[server] != args.LastIncludedIndex {
					rf.matchIndex[server] = args.LastIncludedIndex
					rf.updateCommitIndex()
					rf.commitCond.Broadcast()
				}
				logger.Debugf("%s[%d] received a InstallSnapshotReply in Term[%d] with newer Term[%d] from server[%d]. Install snapshot at Offset[%d] successfully. Update nextIndex[%d] and matchIndex[%d]",
					rf.state, rf.me, *rf.CurrentTerm, reply.Term, server, offset, rf.nextIndex[server], rf.matchIndex[server])
			} else {
				logger.Debugf("%s[%d] received a InstallSnapshotReply in Term[%d] with newer Term[%d] from server[%d]. Install snapshot at Offset[%d] successfully.",
					rf.state, rf.me, *rf.CurrentTerm, reply.Term, server, offset)
			}
		}
		rf.stateCond.L.Unlock()
	}

	return true
}

func (rf *Raft) Snapshot() {

	go func() {
		rf.snapshotChan <- true
	}()
}

func (rf *Raft) storeSnapshot(snapshot Snapshot) {
	if snapshot.LastIncludedIndex <= rf.lastSnapshotIndex {
		return
	}
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(snapshot.LastIncludedIndex)
	e.Encode(snapshot.LastIncludedTerm)
	e.Encode(snapshot.State)
	content := w.Bytes()
	logger.Debugf("%s[%d] store Snapshot at LastSnapshotIndex[%d] with LastSnapshotTerm[%d] in Term[%d]",
		rf.state, rf.me, rf.lastSnapshotIndex, snapshot.LastIncludedTerm, *rf.CurrentTerm)
	rf.persister.SaveSnapshot(content)

	//Discard logs covered by snapshot
	newLog := new([]Entry)
	*newLog = append(*newLog, Entry{Term: snapshot.LastIncludedTerm, Index: snapshot.LastIncludedIndex})
	*newLog = append(*newLog, (*rf.Log)[snapshot.LastIncludedIndex+1-rf.lastSnapshotIndex:]...)
	rf.Log = newLog

	rf.lastSnapshotIndex = snapshot.LastIncludedIndex
	rf.lastSnapshotTerm = snapshot.LastIncludedTerm
}

func (rf *Raft) loadSnapshot(data []byte) {
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	snapshot := Snapshot{}
	d.Decode(&snapshot.LastIncludedIndex)
	d.Decode(&snapshot.LastIncludedTerm)
	rf.commitIndex = snapshot.LastIncludedIndex
	rf.lastApplied = snapshot.LastIncludedIndex

	newLog := new([]Entry)
	*newLog = append(*newLog, Entry{Term: snapshot.LastIncludedTerm, Index: snapshot.LastIncludedIndex})
	if rf.lastSnapshotIndex < snapshot.LastIncludedIndex && snapshot.LastIncludedIndex < rf.lastNewEntryIndex {
		*newLog = append(*newLog, (*rf.Log)[snapshot.LastIncludedIndex+1-rf.lastSnapshotIndex:]...)
		rf.Log = newLog
	} else {
		rf.lastNewEntryIndex = snapshot.LastIncludedIndex
		rf.Log = newLog
	}

	rf.lastSnapshotIndex = snapshot.LastIncludedIndex
	rf.lastSnapshotTerm = snapshot.LastIncludedTerm

	logger.Debugf("%s[%d] load Snapshot, LastIncludedIndex[%d], LastIncludedTerm[%d]",
		rf.state, rf.me, snapshot.LastIncludedIndex, snapshot.LastIncludedTerm)
}

func (rf *Raft) notifyReplica() {

	for i := 0; i < rf.peers.Len(); i++ {
		if i != rf.me {
			go func(i int) {
				rf.stateCond.L.Lock()
				if rf.state == Leader && len((*rf.logReplicaChan)[i]) == 0 {
					select {
					case (*rf.logReplicaChan)[i] <- true:
						break
					default:
					}
				}
				rf.stateCond.L.Unlock()
			}(i)
		}
	}
}

func (rf *Raft) hearBeat() {
	for {
		rf.stateCond.L.Lock()
		for rf.state != Leader {
			rf.stateCond.Wait()
		}
		if rf.state == Dead {
			break
		}
		logger.Debugf("%s[%d] send heartbeat", rf.state, rf.me)
		rf.notifyReplica()
		rf.stateCond.L.Unlock()
		time.Sleep(time.Duration(HeartBeatInterval) * time.Millisecond)
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

func (rf *Raft) initLeader() {
	logLength := len(*rf.Log)
	rf.nextIndex = make(map[int]int)
	rf.matchIndex = make(map[int]int)
	for i := 0; i < rf.peers.Len(); i++ {
		rf.nextIndex[i] = logLength
		rf.matchIndex[i] = 0
	}
	if rf.onLeaderInitCallback != nil {
		rf.onLeaderInitCallback()
	}
	rf.replicateLogs()
}

func (rf *Raft) endLeader() {
	if rf.onLeaderEndCallback != nil {
		rf.onLeaderEndCallback()
	}
	go rf.endReplications()
}

func (rf *Raft) replicateLogs() {
	rf.replicaEnd.Wait()
	logReplicaChan := make([] chan bool, rf.peers.Len())
	rf.logReplicaChan = &logReplicaChan
	for i := 0; i < rf.peers.Len(); i++ {
		if i != rf.me {
			(*rf.logReplicaChan)[i] = make(chan bool)
			go rf.replicateLogsToServer(i)
		}
	}

}

func (rf *Raft) replicateLogsToServer(i int) {
	rf.replicaEnd.Add(1)

	for {
		proceed := <-(*rf.logReplicaChan)[i]

		if !proceed {
			break
		}

		rf.stateCond.L.Lock()
		if rf.state != Leader {
			rf.stateCond.L.Unlock()
			continue
		}

		if rf.nextIndex[i] > rf.lastSnapshotIndex {
			args := new(AppendEntriesArgs)
			reply := new(AppendEntriesReply)
			args.Term = *rf.CurrentTerm
			args.LeaderId = rf.me
			args.PrevLogIndex = rf.nextIndex[i] - 1
			args.LeaderCommit = rf.commitIndex
			if args.PrevLogIndex >= 0 && rf.lastNewEntryIndex >= args.PrevLogIndex {
				args.PrevLogTerm = rf.getEntry(args.PrevLogIndex).Term
			} else {
				args.PrevLogTerm = 0
			}
			args.Entries = (*rf.Log)[(rf.nextIndex[i] - rf.lastSnapshotIndex):(rf.lastNewEntryIndex + 1 - rf.lastSnapshotIndex)]
			go rf.sendAppendEntries(i, args, reply)
		} else {
			args := new(InstallSnapshotArgs)
			reply := new(InstallSnapshotReply)
			args.Term = *rf.CurrentTerm
			args.LastIncludedIndex = rf.lastSnapshotIndex
			args.LastIncludedTerm = rf.lastSnapshotTerm
			args.LeaderId = rf.me
			args.Data = rf.persister.ReadSnapshot()
			go rf.sendInstallSnapshot(i, args, reply)
		}

		rf.stateCond.L.Unlock()
	}
	rf.replicaEnd.Done()
}

func (rf *Raft) endReplications() {
	for i := 0; i < rf.peers.Len(); i++ {
		if i != rf.me {
			(*rf.logReplicaChan)[i] <- false
		}
	}
	rf.stateCond.L.Lock()

	for i, ch := range *rf.logReplicaChan {
		if i != rf.me {
			close(ch)
		}
	}
	rf.stateCond.L.Unlock()
}

func (rf *Raft) commitLogs() {
	if rf.persister.ReadSnapshot() != nil {
		rf.applyCh <- ApplyMsg{UseSnapshot: true, Snapshot: rf.persister.ReadSnapshot()}
	}
	for {
		rf.commitCond.L.Lock()
		if rf.state != Dead {
			select {
			case snapshot := <-rf.snapshotApplyChan:
				applyMsg := ApplyMsg{UseSnapshot: true, Snapshot: snapshot}
				rf.commitCond.L.Unlock()
				rf.applyCh <- applyMsg
				rf.commitCond.L.Lock()
				break
			case <-rf.snapshotChan:
				state := rf.onSnapshotCallback()
				if state == nil {
					break
				}
				snapshot := Snapshot{
					LastIncludedIndex: rf.lastApplied,
					LastIncludedTerm:  rf.getEntry(rf.lastApplied).Term,
					State:             state,
				}
				rf.storeSnapshot(snapshot)
				rf.persist()
				break
			default:
				if rf.commitIndex > rf.lastApplied {
					newApplied := rf.lastApplied + 1
					applyMsg := ApplyMsg{Index: newApplied, Command: rf.getEntry(newApplied).Command, UseSnapshot: false}
					logger.Debugf("%s[%d] commit entry[%d]{%v}", rf.state, rf.me, newApplied, applyMsg.Command)
					rf.lastApplied = newApplied
					rf.commitCond.L.Unlock()
					rf.applyCh <- applyMsg
					rf.commitCond.L.Lock()
				} else {
					rf.commitCond.Wait()
				}
				break
			}
		} else {
			rf.commitCond.L.Unlock()
			break
		}
		rf.commitCond.L.Unlock()
	}
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	rf.stateCond.L.Lock()
	if rf.state == Leader {
		isLeader = true
		index = rf.lastNewEntryIndex + 1
		term = *rf.CurrentTerm
		rf.appendEntry(index, Entry{command, term, index})
		rf.lastNewEntryIndex = index
		rf.nextIndex[rf.me] = index + 1
		rf.matchIndex[rf.me] = index
		rf.persist()
		rf.notifyReplica()
		logger.Debugf("Client send command {%v} to %s[%d]. Success.Update nextIndex[%d] and matchIndex[%d]",
			command, rf.state, rf.me, rf.nextIndex[rf.me], rf.matchIndex[rf.me])
	} else {
		logger.Debugf("Client send command {%v} to %s[%d]. Fail.", command, rf.state, rf.me)
		isLeader = false
	}
	rf.stateCond.L.Unlock()

	return index, term, isLeader
}

func (rf *Raft) updateCommitIndex() {
	oldCommitIndex := rf.commitIndex
	for n := rf.commitIndex + 1; n <= rf.lastNewEntryIndex; n++ {
		sum := 0
		for _, v := range rf.matchIndex {
			if v >= n {
				sum += 1
			}
		}

		if sum > rf.peers.Len()/2 && rf.getEntry(n).Term == *rf.CurrentTerm {
			rf.commitIndex = n
		}
	}

	if oldCommitIndex < rf.commitIndex {
		logger.Debugf("%s[%d] update commitIndex to %d", rf.state, rf.me, rf.commitIndex)
		rf.commitCond.Broadcast()
	}
}

func (rf *Raft) appendEntry(index int, entry Entry) bool {
	i := index - rf.lastSnapshotIndex
	if i < len(*rf.Log) {
		if (*rf.Log)[i].Term != entry.Term {
			(*rf.Log)[i] = entry
			return true
		}
	} else {
		*rf.Log = append(*rf.Log, entry)
		return true
	}
	return false
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	go func() {
		logger.Debugf("Server[%d] is dead", rf.me)
		rf.stateCond.L.Lock()
		rf.toState(Dead)
		rf.stateCond.L.Unlock()
	}()
}

func (rf *Raft) StateSize() int {
	return rf.persister.StateSize()
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
func Make(peers rpc.Peers, me int,
	persister data.Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	// The server initialize as a Follower
	rf.snapshotApplyChan = make(chan []byte)
	rf.snapshotChan = make(chan bool)
	rf.state = Follower
	rf.stateCond = sync.NewCond(&rf.mu)
	rf.commitCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister)
	logging.SetLevel(logging.INFO, "raft")
	logging.SetLevel(logging.INFO, "rpc/mock")

	go rf.hearBeat()
	go rf.electionTimeout()
	go rf.commitLogs()

	return rf
}

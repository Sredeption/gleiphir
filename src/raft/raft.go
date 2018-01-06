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
	"bytes"
	"data"
	"encoding/gob"
	"github.com/op/go-logging"
	"rpc"
	"sync"
	"time"
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
	logReplicaChan *[]chan bool
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

func (rf *Raft) Snapshot() {

	go func() {
		rf.snapshotChan <- true
	}()
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

	go rf.hearBeat()
	go rf.electionTimeout()
	go rf.commitLogs()

	return rf
}

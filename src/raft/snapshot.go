package raft

import (
	"bytes"
	"encoding/gob"
)

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
			args.Data = snapshot[offset : offset+SnapshotChunkSize]
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

package raft

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
		if rf.lastNewEntryIndex < args.PrevLogIndex || (args.PrevLogIndex >= rf.lastApplied && rf.getEntry(args.PrevLogIndex).Term != args.PrevLogTerm) {
			reply.Success = false
			reply.Inconsistent = true
			if rf.lastNewEntryIndex < args.PrevLogIndex {
				reply.FirstIndex = rf.lastNewEntryIndex
				reply.ConflictTerm = rf.getEntry(reply.FirstIndex).Term
			} else {
				reply.ConflictTerm = rf.getEntry(args.PrevLogIndex).Term
				reply.FirstIndex = args.PrevLogIndex
				for reply.FirstIndex > rf.lastSnapshotIndex && rf.getEntry(reply.FirstIndex-1).Term == reply.ConflictTerm {
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

func (rf *Raft) replicateLogs() {
	rf.replicaEnd.Wait()
	logReplicaChan := make([]chan bool, rf.peers.Len())
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

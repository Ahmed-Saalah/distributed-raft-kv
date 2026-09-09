package raft

import (
	"context"
	"log/slog"
	"time"

	pb "github.com/Ahmed-Saalah/distributed-raft-kv/proto"
)

// entry poing for the app to submit a command to the cluster
// returns index that the command will appear at it (commited, current term, leader)
func (rf *Raft) Start(command []byte) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return -1, rf.currentTerm, false
	}

	index := rf.getLastLogIndex() + 1
	term := rf.currentTerm
	rf.log = append(rf.log, &pb.LogEntry{Term: int32(term), Command: command})
	slog.Info("Leader received new command", "node", rf.me, "index", index, "term", term)
	rf.persist()
	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index + 1

	select {
	case rf.triggerAE <- true:
	default:
	}

	return index, term, true
}

func (rf *Raft) StartHeartbeats() {
	for {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}

		term := rf.currentTerm
		me := rf.me
		rf.mu.Unlock()

		for i := range rf.peers {
			if i == me {
				continue
			}

			go func(peer int) {
				rf.mu.Lock()
				if rf.state != Leader {
					rf.mu.Unlock()
					return
				}

				// If the follower needs an index we've already deleted, send a snapshot
				if rf.nextIndex[peer] <= rf.lastIncludedIndex {
					args := &pb.InstallSnapshotArgs{
						Term:              int32(term),
						LeaderId:          int32(me),
						LastIncludedIndex: int32(rf.lastIncludedIndex),
						LastIncludedTerm:  int32(rf.lastIncludedTerm),
						Data:              rf.persister.ReadSnapshot(),
					}
					rf.mu.Unlock()

					ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
					defer cancel()

					reply, err := rf.peers[peer].InstallSnapshot(ctx, args)
					if err != nil {
						return
					}

					rf.mu.Lock()
					defer rf.mu.Unlock()
					if rf.state != Leader || rf.currentTerm != term {
						return
					}
					if reply.Term > int32(rf.currentTerm) {
						rf.currentTerm = int(reply.Term)
						rf.state = Follower
						rf.votedFor = -1
						rf.persist()
						return
					}
					if int(args.LastIncludedIndex) > rf.matchIndex[peer] {
						rf.matchIndex[peer] = int(args.LastIncludedIndex)
					}
					if int(args.LastIncludedIndex)+1 > rf.nextIndex[peer] {
						rf.nextIndex[peer] = int(args.LastIncludedIndex) + 1
					}
					return
				}

				// Standard AppendEntries flow
				prevLogIndex := rf.nextIndex[peer] - 1
				prevLogTerm := rf.getLogTerm(prevLogIndex)
				localPrevLogIndex := rf.getLocalIndex(prevLogIndex)

				entries := make([]*pb.LogEntry, len(rf.log[localPrevLogIndex+1:]))
				copy(entries, rf.log[localPrevLogIndex+1:])

				args := &pb.AppendEntriesArgs{
					LeaderId:     int32(me),
					Term:         int32(term),
					PrevLogIndex: int32(prevLogIndex),
					PrevLogTerm:  int32(prevLogTerm),
					Entries:      entries,
					LeaderCommit: int32(rf.commitIndex),
				}
				rf.mu.Unlock()

				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()

				reply, err := rf.peers[peer].AppendEntries(ctx, args)
				if err != nil {
					return
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()

				if rf.state != Leader || rf.currentTerm != term {
					return
				}

				if reply.Term > int32(rf.currentTerm) {
					rf.currentTerm = int(reply.Term)
					rf.state = Follower
					rf.votedFor = -1
					rf.persist()
					return
				}

				if reply.Success {
					rf.matchIndex[peer] = int(args.PrevLogIndex) + len(args.Entries)
					rf.nextIndex[peer] = rf.matchIndex[peer] + 1

					// Check if we can advance the leader's commitIndex
					for N := rf.getLastLogIndex(); N > rf.commitIndex; N-- {
						if rf.getLogTerm(N) == rf.currentTerm {
							matchCount := 1
							for j := range rf.peers {
								if j != rf.me && rf.matchIndex[j] >= N {
									matchCount++
								}
							}
							if matchCount > len(rf.peers)/2 {
								rf.commitIndex = N
								break
							}
						}
					}
				} else {
					// Follower rejected the log, execute fast-backup optimization
					if reply.XTerm == -1 {
						// follower log is too short
						rf.nextIndex[peer] = int(reply.XLen)
					} else {
						lastMatchIndex := -1
						for idx := rf.getLastLogIndex(); idx > rf.lastIncludedIndex; idx-- {
							if rf.getLogTerm(idx) == int(reply.XTerm) {
								lastMatchIndex = idx
								break
							}
						}
						if lastMatchIndex != -1 {
							// leader has the term, set nextIndex to leader's last entry for XTerm + 1
							rf.nextIndex[peer] = lastMatchIndex + 1
						} else {
							// leader doesn't have the term, set nextIndex to the follower's first conflicting index
							rf.nextIndex[peer] = int(reply.XIndex)
						}
					}
				}
			}(i)
		}

		select {
		case <-time.After(100 * time.Millisecond): // normal beats
		case <-rf.triggerAE: // instant from start()
		}
	}
}

// AppendEntries handles the incoming RPC from the leader
func (rf *Raft) AppendEntries(ctx context.Context, args *pb.AppendEntriesArgs) (*pb.AppendEntriesReply, error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply := &pb.AppendEntriesReply{}

	if args.Term > int32(rf.currentTerm) {
		slog.Debug("Stepping down, received AppendEntries with higher term", "node", rf.me, "currentTerm", rf.currentTerm, "newTerm", args.Term)
		rf.currentTerm = int(args.Term)
		rf.state = Follower
		rf.votedFor = -1
		rf.persist()
	}

	reply.Term = int32(rf.currentTerm)
	reply.Success = false

	if args.Term < int32(rf.currentTerm) {
		return reply, nil
	}

	rf.state = Follower
	rf.lastActive = time.Now()

	// If the leader's prevLogIndex is older than our snapshot we can't append it
	if int(args.PrevLogIndex) < rf.lastIncludedIndex {
		reply.XLen = int32(rf.getLastLogIndex() + 1)
		reply.XTerm = -1
		return reply, nil
	}

	// I don't have an entry at prevLogIndex
	if rf.getLastLogIndex() < int(args.PrevLogIndex) {
		reply.XLen = int32(rf.getLastLogIndex() + 1)
		reply.XTerm = -1
		reply.XIndex = -1
		return reply, nil
	}

	// terms conflict at prevLogIndex
	if rf.getLogTerm(int(args.PrevLogIndex)) != int(args.PrevLogTerm) {
		reply.XLen = int32(rf.getLastLogIndex() + 1)
		reply.XTerm = int32(rf.getLogTerm(int(args.PrevLogIndex)))
		reply.XIndex = args.PrevLogIndex

		for reply.XIndex > int32(rf.lastIncludedIndex+1) && rf.getLogTerm(int(reply.XIndex-1)) == int(reply.XTerm) {
			reply.XIndex--
		}
		return reply, nil
	}

	// truncate conflicting logs and append new entries
	persisted := false
	for i, entry := range args.Entries {
		index := int(args.PrevLogIndex) + 1 + i
		if index <= rf.getLastLogIndex() {
			if rf.getLogTerm(index) != int(entry.Term) {
				rf.log = rf.log[:rf.getLocalIndex(index)]
				rf.log = append(rf.log, entry)
				persisted = true
			}
		} else {
			rf.log = append(rf.log, entry)
			persisted = true
		}
	}
	if persisted {
		rf.persist()
	}

	if int(args.LeaderCommit) > rf.commitIndex {
		lastNewEntry := rf.getLastLogIndex()
		rf.commitIndex = min(int(args.LeaderCommit), lastNewEntry)
	}

	reply.Success = true
	return reply, nil
}

// applier continuously checks for committed logs and sends them to the KV store
func (rf *Raft) applier() {
	for {
		rf.mu.Lock()
		if rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			localIndex := rf.getLocalIndex(rf.lastApplied)
			entry := rf.log[localIndex]

			msg := ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: rf.lastApplied,
			}
			rf.mu.Unlock()
			rf.applyCh <- msg
		} else {
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
		}
	}
}

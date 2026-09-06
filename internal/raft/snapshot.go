package raft

import (
	"bytes"
	"context"
	"encoding/gob"
	"time"

	pb "github.com/Ahmed-Saalah/distributed-raft-kv/proto"
)

// InstallSnapshot is the gRPC Server implementation for handling incoming snapshot transfers.
func (rf *Raft) InstallSnapshot(ctx context.Context, args *pb.InstallSnapshotArgs) (*pb.InstallSnapshotReply, error) {
	rf.mu.Lock()

	reply := &pb.InstallSnapshotReply{}

	if args.Term > int32(rf.currentTerm) {
		rf.currentTerm = int(args.Term)
		rf.state = Follower
		rf.votedFor = -1
		rf.persist()
	}

	reply.Term = int32(rf.currentTerm)
	if args.Term < int32(rf.currentTerm) {
		rf.mu.Unlock()
		return reply, nil
	}

	rf.state = Follower
	rf.lastActive = time.Now()

	// If we already have this snapshot committed, ignore it
	if int(args.LastIncludedIndex) <= rf.commitIndex {
		rf.mu.Unlock()
		return reply, nil
	}

	// Trim the log or clear it if the snapshot contains newer data than our entire log
	if int(args.LastIncludedIndex) >= rf.getLastLogIndex() {
		rf.log = make([]*pb.LogEntry, 1)
	} else {
		newLog := make([]*pb.LogEntry, 1)
		newLog = append(newLog, rf.log[rf.getLocalIndex(int(args.LastIncludedIndex))+1:]...)
		rf.log = newLog
	}

	rf.log[0] = &pb.LogEntry{Term: args.LastIncludedTerm}
	rf.lastIncludedIndex = int(args.LastIncludedIndex)
	rf.lastIncludedTerm = int(args.LastIncludedTerm)

	if int(args.LastIncludedIndex) > rf.commitIndex {
		rf.commitIndex = int(args.LastIncludedIndex)
	}
	if int(args.LastIncludedIndex) > rf.lastApplied {
		rf.lastApplied = int(args.LastIncludedIndex)
	}

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)

	rf.persister.Save(w.Bytes(), args.Data)

	// Send to application state machine
	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  int(args.LastIncludedTerm),
		SnapshotIndex: int(args.LastIncludedIndex),
	}

	rf.mu.Unlock()

	rf.applyCh <- msg

	return reply, nil
}

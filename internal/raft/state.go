package raft

import (
	"bytes"
	"encoding/gob"

	pb "github.com/Ahmed-Saalah/distributed-raft-kv/proto"
)

// Converts a logical/global index to a physical array size index
func (rf *Raft) getLocalIndex(globalIndex int) int {
	return globalIndex - rf.lastIncludedIndex
}

// Gets the term of a specific global index
func (rf *Raft) getLogTerm(globalIndex int) int {
	logIndex := rf.getLocalIndex(globalIndex)
	return int(rf.log[logIndex].Term)
}

// Gets the absolute index of the very last entry in the log
func (rf *Raft) getLastLogIndex() int {
	return rf.lastIncludedIndex + len(rf.log) - 1
}

// Get the term of the very last entry in the log
func (rf *Raft) getLastLogTerm() int {
	return int(rf.log[len(rf.log)-1].Term)
}

// Save raft persist state to storage
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	raftstate := w.Bytes()

	rf.persister.Save(raftstate, rf.persister.ReadSnapshot())
}

// restore previously persisted state
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)

	var currentTerm, votedFor, lastIncludedIndex, lastIncludedTerm int
	var log []*pb.LogEntry

	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil {
		// TODO: log
		return
	}

	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = log
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
}

// how many bytes in Raft's persisted log
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.ReadStateSize()
}

// Snapshot truncates the log when the state machine triggers a compaction
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// If the snapshot is older or equal to what we already have, ignore it
	if index <= rf.lastIncludedIndex {
		return
	}

	localIndex := rf.getLocalIndex(index)
	// Save the term of the last snapshotted entry before truncating
	rf.lastIncludedTerm = int(rf.log[localIndex].Term)
	rf.lastIncludedIndex = index

	// Discard old entries, keeping only the tail.
	newLog := make([]*pb.LogEntry, 1)
	newLog[0] = &pb.LogEntry{Term: int32(rf.lastIncludedTerm), Command: nil}
	newLog = append(newLog, rf.log[localIndex+1:]...)
	rf.log = newLog

	// Persist the Raft state AND the application snapshot simultaneously
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)

	rf.persister.Save(w.Bytes(), snapshot)
}

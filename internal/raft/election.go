package raft

import (
	"context"
	"log"
	"math/rand"
	"time"

	pb "github.com/Ahmed-Saalah/distributed-raft-kv/proto"
)

// ticker runs in the background and triggers elections if heartbeats are missed
func (rf *Raft) ticker() {
	timeout := time.Duration(300+rand.Intn(200)) * time.Millisecond
	for {
		rf.mu.Lock()
		state := rf.state
		lastActive := rf.lastActive
		rf.mu.Unlock()

		if state != Leader && time.Since(lastActive) > timeout {
			rf.mu.Lock()
			rf.lastActive = time.Now()
			rf.mu.Unlock()
			go rf.startElection()
			timeout = time.Duration(300+rand.Intn(200)) * time.Millisecond
		}

		time.Sleep(10 * time.Millisecond) // prevent busy waiting
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.lastActive = time.Now()
	rf.persist()

	log.Printf("Node %d starting election for term %d", rf.me, rf.currentTerm)
	term := rf.currentTerm
	me := rf.me
	rf.mu.Unlock()

	votes := 1

	for i := range rf.peers {
		if i == me {
			continue
		}

		go func(peer int) {
			rf.mu.Lock()
			lastLogIndex := rf.getLastLogIndex()
			lastLogTerm := rf.getLastLogTerm()
			rf.mu.Unlock()

			args := &pb.RequestVoteArgs{
				Term:         int32(term),
				CandidateId:  int32(me),
				LastLogIndex: int32(lastLogIndex),
				LastLogTerm:  int32(lastLogTerm),
			}

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			reply, err := rf.peers[peer].RequestVote(ctx, args)

			if err != nil {
				log.Printf("Node %d failed to reach peer %d: %v", me, peer, err)
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.state != Candidate || term != rf.currentTerm {
				return
			}

			if reply.Term > int32(rf.currentTerm) {
				rf.currentTerm = int(reply.Term)
				rf.state = Follower
				rf.votedFor = -1
				rf.persist()
				return
			}

			if reply.VoteGranted {
				votes++
				if votes > len(rf.peers)/2 {
					log.Printf("Node %d WON election for term %d! Starting heartbeats...", rf.me, rf.currentTerm)
					rf.state = Leader
					for j := range rf.peers {
						rf.nextIndex[j] = rf.getLastLogIndex() + 1
						rf.matchIndex[j] = 0
					}
					go rf.StartHeartbeats()
				}
			}
		}(i)
	}
}

// server implementation for handling incoming request

func (rf *Raft) RequestVote(ctx context.Context, args *pb.RequestVoteArgs) (*pb.RequestVoteReply, error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply := &pb.RequestVoteReply{}

	if args.Term > int32(rf.currentTerm) {
		rf.currentTerm = int(args.Term)
		rf.state = Follower
		rf.votedFor = -1
		rf.persist()
	}

	reply.Term = int32(rf.currentTerm)
	reply.VoteGranted = false

	if args.Term < int32(rf.currentTerm) {
		return reply, nil
	}

	// Election restriction check if candidate's log is at least as up-to-date as ours
	lastLogIndex := rf.getLastLogIndex()
	lastLogTerm := rf.getLastLogTerm()

	if args.LastLogIndex < int32(lastLogIndex) || (args.LastLogTerm == int32(lastLogTerm) && args.LastLogIndex < int32(lastLogIndex)) {
		return reply, nil
	}

	if rf.votedFor == -1 || rf.votedFor == int(args.CandidateId) {
		rf.votedFor = int(args.CandidateId)
		rf.lastActive = time.Now()
		reply.VoteGranted = true
		rf.persist()
	}

	return reply, nil
}

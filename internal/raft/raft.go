package raft

import (
	"sync"
	"time"

	pb "github.com/Ahmed-Saalah/distributed-raft-kv/proto"
)

// routes commited commands back to the RSM
type ApplyMsg struct {
	CommandValid bool
	Command      []byte
	CommandIndex int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type ServerState int

const (
	Follower ServerState = iota
	Candidate
	Leader
)

type Storage interface {
	Save(ragtState []byte, snapshot []byte) error
	ReadRaftState() []byte
	ReadSnapshot() []byte
	ReadStateSize() int
}

type Raft struct {
	pb.UnimplementedRaftServer
	mu        sync.Mutex
	peers     []pb.RaftClient
	me        int
	persister Storage

	// persistent state
	currentTerm int
	votedFor    int
	log         []*pb.LogEntry

	// volatile state on all servers
	commitIndex int
	lastApplied int
	state       ServerState
	lastActive  time.Time

	// volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// snapshot state
	lastIncludedIndex int
	lastIncludedTerm  int

	applyCh   chan ApplyMsg
	triggerAE chan bool
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

func NewRaftNode(peers []pb.RaftClient, me int, persister Storage, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.lastActive = time.Now()

	// init log with a dummy entry to 1-index the log
	rf.log = make([]*pb.LogEntry, 1)
	rf.log[0] = &pb.LogEntry{Term: 0}

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.triggerAE = make(chan bool, 1)

	// init from state persisted before a crach
	// rf.readPersist(persister.ReadRaftState())

	rf.commitIndex = rf.lastIncludedIndex
	rf.lastApplied = rf.lastIncludedIndex

	// Background workers
	go rf.ticker()
	go rf.applier()
	return rf
}

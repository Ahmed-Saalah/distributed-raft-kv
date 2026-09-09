package kvstore

import (
	"bytes"
	"context"
	"encoding/gob"
	"log/slog"
	"sync"
	"time"

	"github.com/Ahmed-Saalah/distributed-raft-kv/internal/raft"
	pb "github.com/Ahmed-Saalah/distributed-raft-kv/proto"
)

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrVersion     = "ErrVersion"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
)

type valueState struct {
	Value   string
	Version uint64
}

type Op struct {
	Id      int64
	Type    string // "Put" or "Get"
	Key     string
	Value   string
	Version uint64
}

type OpResult struct {
	Id      int64
	Err     string
	Value   string
	Version uint64
}

type KVServer struct {
	pb.UnimplementedKVStoreServer

	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	data      map[string]valueState
	waitChans map[int]chan OpResult
	maxRaftState int
}

func NewKVServer(me int, rf *raft.Raft, applyCh chan raft.ApplyMsg, maxRaftState int) *KVServer {
	kv := &KVServer{
		me:           me,
		rf:           rf,
		applyCh:      applyCh,
		data:         make(map[string]valueState),
		waitChans:    make(map[int]chan OpResult),
		maxRaftState: maxRaftState,
	}

	go kv.applier()
	return kv
}

// listens for committed commands from Raft.
func (kv *KVServer) applier() {
	for msg := range kv.applyCh {
		if msg.SnapshotValid {
			kv.Restore(msg.Snapshot)
		} else if msg.CommandValid {
			r := bytes.NewBuffer(msg.Command)
			d := gob.NewDecoder(r)
			var op Op

			if d.Decode(&op) != nil {
				continue
			}

			kv.mu.Lock()
			slog.Debug("Applying operation from Raft", "node", kv.me, "opType", op.Type, "key", op.Key)
			result := kv.applyOp(op)
			result.Id = op.Id

			// If an RPC thread is waiting on this index, send it the result
			ch, ok := kv.waitChans[msg.CommandIndex]
			if ok {
				ch <- result
			}

			if kv.maxRaftState != -1 && kv.rf.PersistBytes() > kv.maxRaftState {
				slog.Info("Log size exceeded threshold, triggering snapshot", "node", kv.me, "size", kv.rf.PersistBytes(), "index", msg.CommandIndex)
				w := new(bytes.Buffer)
				e := gob.NewEncoder(w)
				if err := e.Encode(kv.data); err == nil {
					kv.rf.Snapshot(msg.CommandIndex, w.Bytes())
				} else {
					slog.Error("Failed to encode snapshot data", "error", err)
				}
			}

			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) applyOp(op Op) OpResult {
	if op.Type == "Get" {
		state, exists := kv.data[op.Key]
		if exists {
			return OpResult{Value: state.Value, Version: state.Version, Err: OK}
		}
		return OpResult{Err: ErrNoKey}
	}

	if op.Type == "Put" {
		state, exists := kv.data[op.Key]
		if !exists {
			if op.Version != 0 {
				return OpResult{Err: ErrVersion}
			}
			kv.data[op.Key] = valueState{Value: op.Value, Version: 1}
			return OpResult{Err: OK}
		}

		if state.Version != op.Version {
			return OpResult{Err: ErrVersion}
		}

		kv.data[op.Key] = valueState{Value: op.Value, Version: state.Version + 1}
		return OpResult{Err: OK}
	}

	return OpResult{Err: "UnknownOp"}
}

func (kv *KVServer) submitAndWait(op Op) OpResult {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(op)

	index, _, isLeader := kv.rf.Start(w.Bytes())
	if !isLeader {
		return OpResult{Err: ErrWrongLeader}
	}
	slog.Info("Submitted operation to Raft", "node", kv.me, "opType", op.Type, "key", op.Key, "index", index)

	kv.mu.Lock()
	ch := make(chan OpResult, 1)
	kv.waitChans[index] = ch
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		delete(kv.waitChans, index)
		kv.mu.Unlock()
	}()

	select {
	case result := <-ch:
		return result
	case <-time.After(2 * time.Second):
		return OpResult{Err: ErrTimeout}
	}
}

func (kv *KVServer) Get(ctx context.Context, args *pb.GetArgs) (*pb.GetReply, error) {
	op := Op{Type: "Get", Key: args.Key}
	res := kv.submitAndWait(op)
	return &pb.GetReply{Err: res.Err, Value: res.Value, Version: res.Version}, nil
}

func (kv *KVServer) Put(ctx context.Context, args *pb.PutArgs) (*pb.PutReply, error) {
	op := Op{Type: "Put", Key: args.Key, Value: args.Value, Version: args.Version}
	res := kv.submitAndWait(op)
	return &pb.PutReply{Err: res.Err}, nil
}

func (kv *KVServer) Restore(data []byte) {
	if data == nil || len(data) == 0 {
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	var decodedData map[string]valueState
	if d.Decode(&decodedData) == nil {
		kv.data = decodedData
	}
}

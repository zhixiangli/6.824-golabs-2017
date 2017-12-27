package raftkv

import (
	"bytes"
	"encoding/gob"
	"log"
	"sync"
	"time"

	"labrpc"
	"raft"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const (
	OP_GET    = "Get"
	OP_PUT    = "Put"
	OP_APPEND = "Append"

	OP_TIMEOUT = time.Second
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type      string
	Key       string
	Value     string
	ClerkId   int64
	RequestId uint64
}

type RaftKV struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data   map[string]string
	result map[int]chan Op
	acked  map[int64]uint64
}

func (kv *RaftKV) getResultChan(index int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	resultChan, ok := kv.result[index]
	if !ok {
		resultChan = make(chan Op, 1)
		kv.result[index] = resultChan
	}
	return resultChan
}

func (kv *RaftKV) startAgreement(op Op) bool {
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return false
	}
	resultChan := kv.getResultChan(index)
	select {
	case res := <-resultChan:
		return res == op
	case <-time.After(OP_TIMEOUT):
		return false
	}
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Type: OP_GET,
		Key:  args.Key,
	}
	if !kv.startAgreement(op) {
		reply.WrongLeader = true
		return
	}
	kv.mu.RLock()
	value, ok := kv.data[args.Key]
	kv.mu.RUnlock()
	if ok {
		reply.Value = value
		reply.Err = OK
	} else {
		reply.Err = ErrNoKey
	}
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		Type:      args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClerkId:   args.ClerkId,
		RequestId: args.RequestId,
	}
	if !kv.startAgreement(op) {
		reply.WrongLeader = true
		return
	}
	reply.Err = OK
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *RaftKV) applyMsg(op *Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	switch op.Type {
	case OP_PUT:
		kv.data[op.Key] = op.Value
	case OP_APPEND:
		kv.data[op.Key] += op.Value
	}
	kv.acked[op.ClerkId] = op.RequestId
}

func (kv *RaftKV) isAcked(op *Op) bool {
	if maxRequestId, ok := kv.acked[op.ClerkId]; ok {
		return maxRequestId >= op.RequestId
	}
	return false
}

func (kv *RaftKV) encodeSnapshot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(kv.data)
	e.Encode(kv.acked)
	return w.Bytes()
}

func (kv *RaftKV) decodeSnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&kv.data)
	d.Decode(&kv.acked)
}

func (kv *RaftKV) applyMsgLoop() {
	for {
		msg := <-kv.applyCh
		if msg.UseSnapshot {
			kv.decodeSnapshot(msg.Snapshot)
		} else {
			op := msg.Command.(Op)
			if !kv.isAcked(&op) {
				kv.applyMsg(&op)
			}
			resultChan := kv.getResultChan(msg.Index)
			select {
			case <-resultChan:
			default:
			}
			resultChan <- op

			if kv.maxraftstate != -1 && kv.rf.RaftStateSize() > kv.maxraftstate {
				kv.rf.UpdateSnapshot(kv.encodeSnapshot(), msg.Index)
			}
		}
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.data = make(map[string]string)
	kv.result = make(map[int]chan Op)
	kv.acked = make(map[int64]uint64)

	go kv.applyMsgLoop()

	return kv
}

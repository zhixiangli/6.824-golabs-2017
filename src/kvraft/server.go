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
	Type  string
	Args  interface{}
	Reply interface{}
}

type RaftKV struct {
	mu      sync.Mutex
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
	resultChan, ok := kv.result[index]
	if !ok {
		resultChan = make(chan Op, 1)
		kv.result[index] = resultChan
	}
	return resultChan
}

func (kv *RaftKV) startAgreement(op Op) (bool, Op) {
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return false, Op{}
	}
	// wait agreement
	kv.mu.Lock()
	resultChan := kv.getResultChan(index)
	kv.mu.Unlock()
	select {
	case result := <-resultChan:
		return result.Type == op.Type, result
	case <-time.After(OP_TIMEOUT):
		return false, Op{}
	}
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Type: OP_GET,
		Args: *args,
	}
	if isLeader, result := kv.startAgreement(op); !isLeader {
		reply.Meta.WrongLeader = true
	} else {
		meta := result.Args.(GetArgs).Meta
		if meta != args.Meta {
			reply.Meta.WrongLeader = true
		} else {
			*reply = result.Reply.(GetReply)
		}
	}
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		Type: args.Op,
		Args: *args,
	}
	if isLeader, result := kv.startAgreement(op); !isLeader {
		reply.Meta.WrongLeader = true
	} else {
		meta := result.Args.(PutAppendArgs).Meta
		if meta != args.Meta {
			reply.Meta.WrongLeader = true
		} else {
			*reply = result.Reply.(PutAppendReply)
		}
	}
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

func (kv *RaftKV) isAcked(clerkId int64, requestId uint64) bool {
	if maxRequestId, ok := kv.acked[clerkId]; ok {
		return maxRequestId >= requestId
	}
	return false
}

func (kv *RaftKV) applyGet(args GetArgs) (reply GetReply) {
	if value, ok := kv.data[args.Key]; ok {
		reply.Value = value
		reply.Meta.Err = OK
	} else {
		reply.Meta.Err = ErrNoKey
	}
	return
}

func (kv *RaftKV) applyPutAppend(args PutAppendArgs) (reply PutAppendReply) {
	reply.Meta.Err = OK
	if kv.isAcked(args.Meta.ClerkId, args.Meta.RequestId) {
		return
	}
	switch args.Op {
	case OP_PUT:
		kv.data[args.Key] = args.Value
	case OP_APPEND:
		kv.data[args.Key] += args.Value
	}
	kv.acked[args.Meta.ClerkId] = args.Meta.RequestId
	return
}

func (kv *RaftKV) applyOp(op Op) interface{} {
	switch op.Type {
	case OP_GET:
		return kv.applyGet(op.Args.(GetArgs))
	case OP_PUT, OP_APPEND:
		return kv.applyPutAppend(op.Args.(PutAppendArgs))
	}
	return nil
}

func (kv *RaftKV) encodeSnapshot() []byte {
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
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	kv.data = make(map[string]string)
	kv.acked = make(map[int64]uint64)
	d.Decode(&kv.data)
	d.Decode(&kv.acked)
}

func (kv *RaftKV) applyLoop() {
	for {
		msg := <-kv.applyCh
		kv.mu.Lock()
		if msg.UseSnapshot {
			kv.decodeSnapshot(msg.Snapshot)
		} else {
			op := msg.Command.(Op)
			op.Reply = kv.applyOp(op)
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
		kv.mu.Unlock()
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
	gob.Register(GetArgs{})
	gob.Register(PutAppendArgs{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg, 1<<10)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.data = make(map[string]string)
	kv.result = make(map[int]chan Op)
	kv.acked = make(map[int64]uint64)

	go kv.applyLoop()

	return kv
}

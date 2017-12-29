package shardmaster

import (
	"encoding/gob"
	"reflect"
	"sync"
	"time"

	"labrpc"
	"raft"
)

type ShardMaster struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	result map[int]chan Op
	acked  map[int64]uint64

	configs []Config // indexed by config num
}

type Op struct {
	// Your data here.
	Type string
	Args interface{}
}

const (
	OP_JOIN  = "Join"
	OP_LEAVE = "Leave"
	OP_MOVE  = "Move"
	OP_QUERY = "Query"

	OP_TIMEOUT = time.Second
)

func (sm *ShardMaster) getResultChan(index int) chan Op {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	resultChan, ok := sm.result[index]
	if !ok {
		resultChan = make(chan Op, 1)
		sm.result[index] = resultChan
	}
	return resultChan
}

func (sm *ShardMaster) startAgreement(op Op) bool {
	index, _, isLeader := sm.rf.Start(op)
	if !isLeader {
		return false
	}
	resultChan := sm.getResultChan(index)
	select {
	case res := <-resultChan:
		return reflect.DeepEqual(res, op)
	case <-time.After(OP_TIMEOUT):
		return false
	}
}

func (sm *ShardMaster) getLastConfig() Config {
	return sm.configs[len(sm.configs)-1]
}

func (sm *ShardMaster) isAcked(clerkId int64, requestId uint64) bool {
	if maxRequestId, ok := sm.acked[clerkId]; ok {
		return maxRequestId >= requestId
	}
	return false
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op{
		Type: OP_JOIN,
		Args: *args,
	}
	if !sm.startAgreement(op) {
		reply.WrongLeader = true
		return
	}
	reply.Err = OK
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{
		Type: OP_LEAVE,
		Args: *args,
	}
	if !sm.startAgreement(op) {
		reply.WrongLeader = true
		return
	}
	reply.Err = OK
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{
		Type: OP_MOVE,
		Args: *args,
	}
	if !sm.startAgreement(op) {
		reply.WrongLeader = true
		return
	}
	reply.Err = OK
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op{
		Type: OP_QUERY,
		Args: *args,
	}
	if !sm.startAgreement(op) {
		reply.WrongLeader = true
		return
	}
	reply.Err = OK
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	if args.Num < 0 {
		reply.Config = sm.getLastConfig()
	} else {
		reply.Config = sm.configs[args.Num]
	}
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

func (sm *ShardMaster) makeConfig() Config {
	lastConfig := sm.getLastConfig()
	conf := Config{
		Num:    lastConfig.Num + 1,
		Groups: make(map[int][]string),
	}
	for i, gid := range lastConfig.Shards {
		conf.Shards[i] = gid
	}
	for gid, servers := range lastConfig.Groups {
		conf.Groups[gid] = servers
	}
	return conf
}

func (sm *ShardMaster) applyMove(args MoveArgs) {
	conf := sm.makeConfig()
	conf.Shards[args.Shard] = args.GID
	sm.configs = append(sm.configs, conf)
}

func (sm *ShardMaster) getGidFrequency(conf Config) (leastGid, leastFrequency, mostGid, mostFrequency int) {
	countsMap := make(map[int]int)
	for gid, _ := range conf.Groups {
		countsMap[gid] = 0
	}
	for _, gid := range conf.Shards {
		if gid > 0 {
			countsMap[gid]++
		}
	}
	for gid, cnt := range countsMap {
		if leastGid == 0 || countsMap[leastGid] > cnt {
			leastGid = gid
			leastFrequency = cnt
		}
		if mostGid == 0 || countsMap[mostGid] < cnt {
			mostGid = gid
			mostFrequency = cnt
		}
	}
	return
}

func (sm *ShardMaster) applyLeave(args LeaveArgs) {
	conf := sm.makeConfig()
	for _, gid := range args.GIDs {
		delete(conf.Groups, gid)
	}
	for i, gid := range conf.Shards {
		if _, ok := conf.Groups[gid]; !ok {
			conf.Shards[i] = 0
		}
	}
	for i, gid := range conf.Shards {
		if _, ok := conf.Groups[gid]; !ok {
			conf.Shards[i], _, _, _ = sm.getGidFrequency(conf)
		}
	}
	sm.configs = append(sm.configs, conf)
}

func (sm *ShardMaster) applyJoin(args JoinArgs) {
	conf := sm.makeConfig()
	defaultGid := 0
	for gid, servers := range args.Servers {
		conf.Groups[gid] = servers
		defaultGid = gid
	}
	for i, gid := range conf.Shards {
		if gid == 0 {
			conf.Shards[i] = defaultGid
		}
	}
	for {
		leastGid, leastFrequency, mostGid, mostFrequency := sm.getGidFrequency(conf)
		if mostFrequency-leastFrequency <= 1 {
			break
		}
		for i, gid := range conf.Shards {
			if gid == mostGid {
				conf.Shards[i] = leastGid
				break
			}
		}
	}
	sm.configs = append(sm.configs, conf)
}

func (sm *ShardMaster) applyOp(op Op) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	switch op.Type {
	case OP_MOVE:
		args := op.Args.(MoveArgs)
		if sm.isAcked(args.ClerkId, args.RequestId) {
			return
		}
		sm.applyMove(args)
	case OP_LEAVE:
		args := op.Args.(LeaveArgs)
		if sm.isAcked(args.ClerkId, args.RequestId) {
			return
		}
		sm.applyLeave(args)
	case OP_JOIN:
		args := op.Args.(JoinArgs)
		if sm.isAcked(args.ClerkId, args.RequestId) {
			return
		}
		sm.applyJoin(args)
	}
}

func (sm *ShardMaster) applyMsgLoop() {
	for {
		msg := <-sm.applyCh
		op := msg.Command.(Op)
		sm.applyOp(op)
		resultChan := sm.getResultChan(msg.Index)
		select {
		case <-resultChan:
		default:
		}
		resultChan <- op

	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	gob.Register(Op{})
	gob.Register(JoinArgs{})
	gob.Register(LeaveArgs{})
	gob.Register(MoveArgs{})
	gob.Register(QueryArgs{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.result = make(map[int]chan Op)
	sm.acked = make(map[int64]uint64)

	go sm.applyMsgLoop()

	return sm
}

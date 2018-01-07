package shardkv

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"sync"
	"time"

	"labrpc"
	"raft"
	"shardmaster"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const (
	OP_GET            = "Get"
	OP_PUT            = "Put"
	OP_APPEND         = "Append"
	OP_SYNC_GC        = "SyncGC"
	OP_SYNC_SHARD     = "SyncShard"
	OP_UPGRADE_CONFIG = "UpgradeConfig"
	OP_REQUEST_SHARD  = "RequestShard"

	OP_TIMEOUT       = time.Second
	RECONFIG_TIMEOUT = 50 * time.Millisecond
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type  string
	Args  interface{}
	Reply interface{}
}

type ShardStorage struct {
	ConfigNum int
	Data      map[string]string
	Acked     map[int64]uint64
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	mck    *shardmaster.Clerk
	result map[int]chan Op
	config shardmaster.Config
	db     [shardmaster.NShards]ShardStorage
}

func (kv *ShardKV) toString() string {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return fmt.Sprintf("me: %v, gid: %v, config: %v, data: %v", kv.me, kv.gid, kv.config, kv.db)
}

func (kv *ShardKV) isLeader() bool {
	_, isLeader := kv.rf.GetState()
	return isLeader
}

func (kv *ShardKV) getConfigNum() int {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.config.Num
}

func (kv *ShardKV) getResultChan(index int) chan Op {
	resultChan, ok := kv.result[index]
	if !ok {
		resultChan = make(chan Op, 1)
		kv.result[index] = resultChan
	}
	return resultChan
}

func (kv *ShardKV) startAgreement(op Op) (bool, Op) {
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

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
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
	DPrintf("Get args: %v, reply: %v, %s", args, reply, kv.toString())
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
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
	DPrintf("PutAppend args: %v, reply: %v, %s", args, reply, kv.toString())
}

func (kv *ShardKV) SyncGC(args *SyncGCArgs, reply *SyncGCReply) {
	op := Op{
		Type: OP_SYNC_GC,
		Args: *args,
	}
	if isLeader, result := kv.startAgreement(op); !isLeader {
		reply.Meta.WrongLeader = true
	} else {
		meta := result.Args.(SyncGCArgs).Meta
		if meta != args.Meta {
			reply.Meta.WrongLeader = true
		} else {
			*reply = result.Reply.(SyncGCReply)
		}
	}
}

func (kv *ShardKV) RequestShard(args *RequestShardArgs, reply *RequestShardReply) {
	op := Op{
		Type: OP_REQUEST_SHARD,
		Args: *args,
	}
	if isLeader, result := kv.startAgreement(op); !isLeader {
		reply.Meta.WrongLeader = true
	} else {
		meta := result.Args.(RequestShardArgs).Meta
		if meta != args.Meta {
			reply.Meta.WrongLeader = true
		} else {
			*reply = result.Reply.(RequestShardReply)
		}
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) isAcked(shard int, clerkId int64, requestId uint64) bool {
	if maxRequestId, ok := kv.db[shard].Acked[clerkId]; ok {
		return maxRequestId >= requestId
	}
	return false
}

func (kv *ShardKV) isWrongGroup(shard int) bool {
	return kv.config.Shards[shard] != kv.gid
}

func (kv *ShardKV) applyGet(args GetArgs) (reply GetReply) {
	shard := key2shard(args.Key)
	if kv.isWrongGroup(shard) {
		reply.Meta.Err = ErrWrongGroup
	} else if value, ok := kv.db[shard].Data[args.Key]; ok {
		reply.Value = value
		reply.Meta.Err = OK
	} else {
		reply.Meta.Err = ErrNoKey
	}
	return
}

func (kv *ShardKV) applyPutAppend(args PutAppendArgs) (reply PutAppendReply) {
	reply.Meta.Err = OK
	shard := key2shard(args.Key)
	if kv.isAcked(shard, args.Meta.ClerkId, args.Meta.RequestId) {
		return
	}
	if kv.isWrongGroup(shard) {
		reply.Meta.Err = ErrWrongGroup
		return
	}
	switch args.Op {
	case OP_PUT:
		kv.db[shard].Data[args.Key] = args.Value
	case OP_APPEND:
		kv.db[shard].Data[args.Key] += args.Value
	}
	kv.db[shard].Acked[args.Meta.ClerkId] = args.Meta.RequestId
	return
}

func (kv *ShardKV) applyRequestShard(args RequestShardArgs) (reply RequestShardReply) {
	if args.ConfigNum > kv.config.Num {
		reply.Meta.Err = ErrWrongVersion
		return
	}
	reply.Meta.Err = OK
	reply.Data = make(map[string]string)
	reply.Acked = make(map[int64]uint64)
	for key, value := range kv.db[args.Shard].Data {
		reply.Data[key] = value
	}
	for key, value := range kv.db[args.Shard].Acked {
		reply.Acked[key] = value
	}
	if args.ConfigNum == kv.config.Num {
		// if equals, set to 0 to stop reading & writing
		// otherwise, in other configNum, it will be a different value.
		kv.config.Shards[args.Shard] = 0
	}
	return
}

func (kv *ShardKV) applySyncShard(args SyncShardArgs) (reply SyncShardReply) {
	reply.Meta.Err = OK
	if kv.config.Shards[args.Shard] == kv.gid {
		return
	}
	for key, value := range args.Data {
		kv.db[args.Shard].Data[key] = value
	}
	for key, value := range args.Acked {
		if old, ok := kv.db[args.Shard].Acked[key]; !ok || (ok && value > old) {
			kv.db[args.Shard].Acked[key] = value
		}
	}
	kv.db[args.Shard].ConfigNum = kv.config.Num
	kv.config.Shards[args.Shard] = kv.gid
	return
}

func (kv *ShardKV) applyUpgradeConfig(args UpgradeConfigArgs) (reply UpgradeConfigReply) {
	if args.Config.Num > kv.config.Num {
		kv.config = args.Config
	}
	reply.Meta.Err = OK
	return
}

func (kv *ShardKV) applySyncGC(args SyncGCArgs) (reply SyncGCReply) {
	if args.ConfigNum < kv.db[args.Shard].ConfigNum {
		reply.Meta.Err = ErrWrongVersion
		return
	}
	reply.Meta.Err = OK
	kv.db[args.Shard] = ShardStorage{
		Data:  make(map[string]string),
		Acked: make(map[int64]uint64),
	}
	return
}

func (kv *ShardKV) applyOp(op Op) interface{} {
	switch op.Type {
	case OP_GET:
		return kv.applyGet(op.Args.(GetArgs))
	case OP_PUT, OP_APPEND:
		return kv.applyPutAppend(op.Args.(PutAppendArgs))
	case OP_UPGRADE_CONFIG:
		return kv.applyUpgradeConfig(op.Args.(UpgradeConfigArgs))
	case OP_SYNC_GC:
		return kv.applySyncGC(op.Args.(SyncGCArgs))
	case OP_SYNC_SHARD:
		return kv.applySyncShard(op.Args.(SyncShardArgs))
	case OP_REQUEST_SHARD:
		return kv.applyRequestShard(op.Args.(RequestShardArgs))
	}
	return nil
}

func (kv *ShardKV) applyLoop() {
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
			if kv.maxraftstate != -1 && kv.rf.RaftStateSize() >= kv.maxraftstate {
				kv.rf.UpdateSnapshot(kv.encodeSnapshot(), msg.Index)
			}
		}
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) encodeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(kv.db)
	e.Encode(kv.config)
	return w.Bytes()
}

func (kv *ShardKV) decodeSnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	for i := 0; i < shardmaster.NShards; i++ {
		kv.db[i] = ShardStorage{
			Data:  make(map[string]string),
			Acked: make(map[int64]uint64),
		}
	}
	d.Decode(&kv.db)
	d.Decode(&kv.config)
}

func (kv *ShardKV) startRequestShard(servers []string, args *RequestShardArgs, reply *RequestShardReply) bool {
	DPrintf("start request shard to %v, args: %v, %s", servers, args, kv.toString())
	if len(servers) < 1 {
		return true
	}
	for server := 0; server < len(servers); server++ {
		srv := kv.make_end(servers[server])
		var requestShardReply RequestShardReply
		ok := srv.Call("ShardKV.RequestShard", args, &requestShardReply)
		if ok && requestShardReply.Meta.Err == OK && !requestShardReply.Meta.WrongLeader {
			*reply = requestShardReply
			return true
		}
	}
	DPrintf("request shard done! reply: %v, %s", reply, kv.toString())
	return false
}

func (kv *ShardKV) startSyncShard(args *SyncShardArgs, reply *SyncShardReply) bool {
	op := Op{
		Type: OP_SYNC_SHARD,
		Args: *args,
	}
	DPrintf("start sync shard, args: %v, %s", args, kv.toString())
	if isLeader, result := kv.startAgreement(op); !isLeader {
		reply.Meta.WrongLeader = true
	} else {
		meta := result.Args.(SyncShardArgs).Meta
		if meta != args.Meta {
			reply.Meta.WrongLeader = true
		} else {
			*reply = result.Reply.(SyncShardReply)
		}
	}
	DPrintf("sync shard done!, reply: %v, %s", reply, kv.toString())
	return !reply.Meta.WrongLeader && reply.Meta.Err == OK
}

func (kv *ShardKV) startUpgradeConfig(args *UpgradeConfigArgs, reply *UpgradeConfigReply) bool {
	op := Op{
		Type: OP_UPGRADE_CONFIG,
		Args: *args,
	}
	if isLeader, result := kv.startAgreement(op); !isLeader {
		reply.Meta.WrongLeader = true
	} else {
		meta := result.Args.(UpgradeConfigArgs).Meta
		if meta != args.Meta {
			reply.Meta.WrongLeader = true
		} else {
			*reply = result.Reply.(UpgradeConfigReply)
		}
	}
	return !reply.Meta.WrongLeader && reply.Meta.Err == OK
}

func (kv *ShardKV) startSyncGC(servers []string, args *SyncGCArgs, reply *SyncGCReply) bool {
	if len(servers) < 1 {
		return true
	}
	for server := 0; server < len(servers); server++ {
		srv := kv.make_end(servers[server])
		var syncGCReply SyncGCReply
		ok := srv.Call("ShardKV.SyncGC", args, &syncGCReply)
		if ok && !syncGCReply.Meta.WrongLeader && (syncGCReply.Meta.Err == OK || syncGCReply.Meta.Err == ErrWrongVersion) {
			*reply = syncGCReply
			return true
		}
	}
	return false
}

func (kv *ShardKV) reconfig(nextConfig shardmaster.Config) bool {
	waitGroup := sync.WaitGroup{}
	resultChan := make(chan bool, shardmaster.NShards)
	kv.mu.Lock()
	for i := 0; i < shardmaster.NShards; i++ {
		if kv.config.Shards[i] == kv.gid || nextConfig.Shards[i] != kv.gid { // no need to pull shards
			continue
		}
		args := &RequestShardArgs{
			ConfigNum: kv.config.Num,
			Shard:     i,
			Meta: ArgsMeta{
				ClerkId:   int64(kv.gid),
				RequestId: uint64(nrand()),
			},
		}
		servers := append([]string{}, kv.config.Groups[kv.config.Shards[i]]...)
		waitGroup.Add(1)
		go func(servers []string, requestShardArgs *RequestShardArgs) {
			defer waitGroup.Done()
			var requestShardReply RequestShardReply
			if !kv.startRequestShard(servers, requestShardArgs, &requestShardReply) {
				resultChan <- false
				return
			}
			syncShardArgs := &SyncShardArgs{
				ConfigNum: requestShardArgs.ConfigNum,
				Shard:     requestShardArgs.Shard,
				Data:      requestShardReply.Data,
				Acked:     requestShardReply.Acked,
				Meta: ArgsMeta{
					ClerkId:   int64(kv.gid),
					RequestId: uint64(nrand()),
				},
			}
			if !kv.startSyncShard(syncShardArgs, &SyncShardReply{}) {
				resultChan <- false
				return
			}
		}(servers, args)
	}
	kv.mu.Unlock()
	waitGroup.Wait()
	close(resultChan)
	for result := range resultChan {
		if !result {
			return false
		}
	}
	return true
}

func (kv *ShardKV) reconfigLoop() {
	for {
		if !kv.isLeader() {
			time.Sleep(RECONFIG_TIMEOUT)
			continue
		}
		nextConfigNum := kv.getConfigNum() + 1
		nextConfig := kv.mck.Query(nextConfigNum)
		if nextConfig.Num != nextConfigNum {
			time.Sleep(RECONFIG_TIMEOUT)
			continue
		}
		DPrintf("start to reconfig, next config: %v, %s", nextConfig, kv.toString())
		if !kv.reconfig(nextConfig) {
			DPrintf("reconfig fail, next config: %v, %s", nextConfig, kv.toString())
			continue
		}
		DPrintf("reconfig done, next config: %v, %s", nextConfig, kv.toString())
		args := &UpgradeConfigArgs{
			Config: nextConfig,
			Meta: ArgsMeta{
				ClerkId:   int64(kv.gid),
				RequestId: uint64(nrand()),
			},
		}
		DPrintf("start upgrade config, next config: %v, %s", nextConfig, kv.toString())
		if !kv.startUpgradeConfig(args, &UpgradeConfigReply{}) {
			DPrintf("upgrade config fail, next config: %v, %s", nextConfig, kv.toString())
			continue
		}
		DPrintf("upgrade config done, next config: %v, %s", nextConfig, kv.toString())
		go kv.gcLoop(kv.getConfigNum())
	}
}

func (kv *ShardKV) gcLoop(configNum int) {
	waitGroup := sync.WaitGroup{}
	oldConfig, newConfig := kv.mck.Query(configNum-1), kv.mck.Query(configNum)
	for i := 0; i < shardmaster.NShards; i++ {
		if oldConfig.Shards[i] == kv.gid || newConfig.Shards[i] != kv.gid {
			continue
		}
		servers := oldConfig.Groups[oldConfig.Shards[i]]
		if len(servers) < 1 {
			continue
		}
		syncGCArgs := &SyncGCArgs{
			ConfigNum: configNum - 1,
			Shard:     i,
			Meta: ArgsMeta{
				ClerkId:   int64(kv.gid),
				RequestId: uint64(nrand()),
			},
		}
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			var syncGCReply SyncGCReply
			for kv.isLeader() && !kv.startSyncGC(servers, syncGCArgs, &syncGCReply) {
			}
			DPrintf("sync gc to %v, done! args: %v, reply: %v, %s", servers, syncGCArgs, syncGCReply, kv.toString())
		}()
	}
	waitGroup.Wait()
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots with
// persister.SaveSnapshot(), and Raft should save its state (including
// log) with persister.SaveRaftState().
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})
	gob.Register(ShardStorage{})
	gob.Register(GetArgs{})
	gob.Register(PutAppendArgs{})
	gob.Register(UpgradeConfigArgs{})
	gob.Register(SyncGCArgs{})
	gob.Register(SyncShardArgs{})
	gob.Register(RequestShardArgs{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Use something like this to talk to the shardmaster:
	kv.mck = shardmaster.MakeClerk(kv.masters)

	// Your initialization code here.
	for i := 0; i < shardmaster.NShards; i++ {
		kv.db[i] = ShardStorage{
			ConfigNum: -1,
			Data:      make(map[string]string),
			Acked:     make(map[int64]uint64),
		}
	}
	kv.result = make(map[int]chan Op)
	kv.applyCh = make(chan raft.ApplyMsg, 1<<10)
	go kv.applyLoop()
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	go kv.reconfigLoop()
	return kv
}

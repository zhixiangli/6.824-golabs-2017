package shardkv

import "shardmaster"

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK              = "OK"
	ErrNoKey        = "ErrNoKey"
	ErrWrongGroup   = "ErrWrongGroup"
	ErrWrongVersion = "ErrWrongVersion"
)

type Err string

type ArgsMeta struct {
	ClerkId   int64
	RequestId uint64
}

type ReplyMeta struct {
	WrongLeader bool
	Err         Err
}

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Meta ArgsMeta
}

type PutAppendReply struct {
	Meta ReplyMeta
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Meta ArgsMeta
}

type GetReply struct {
	Meta  ReplyMeta
	Value string
}

type UpgradeConfigArgs struct {
	Config shardmaster.Config
	Meta   ArgsMeta
}

type UpgradeConfigReply struct {
	Meta ReplyMeta
}

type RequestShardArgs struct {
	ConfigNum int
	Shard     int
	Meta      ArgsMeta
}

type RequestShardReply struct {
	Data  map[string]string
	Acked map[int64]uint64
	Meta  ReplyMeta
}

type SyncShardArgs struct {
	ConfigNum int
	Shard     int
	Data      map[string]string
	Acked     map[int64]uint64
	Meta      ArgsMeta
}

type SyncShardReply struct {
	Meta ReplyMeta
}

type SyncGCArgs struct {
	ConfigNum int
	Shard     int
	Meta      ArgsMeta
}

type SyncGCReply struct {
	Meta ReplyMeta
}

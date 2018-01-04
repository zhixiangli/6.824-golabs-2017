package raftkv

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
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

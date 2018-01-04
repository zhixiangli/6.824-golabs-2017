package raftkv

import (
	"crypto/rand"
	"math/big"
	"sync/atomic"

	"labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clerkId   int64
	requestId uint64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clerkId = nrand()
	ck.requestId = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	args := &GetArgs{
		Key: key,
		Meta: ArgsMeta{
			ClerkId:   ck.clerkId,
			RequestId: atomic.AddUint64(&ck.requestId, 1),
		},
	}
	for i := 0; ; i = (i + 1) % len(ck.servers) {
		reply := &GetReply{}
		DPrintf("clerkId: %v, requestId: %v, Get args: %v to %v", ck.clerkId, ck.requestId, args, i)
		if ok := ck.servers[i].Call("RaftKV.Get", args, reply); ok && !reply.Meta.WrongLeader {
			DPrintf("clerkId: %v, requestId: %v, Get reply: %v", ck.clerkId, ck.requestId, reply)
			return reply.Value
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := &PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
		Meta: ArgsMeta{
			ClerkId:   ck.clerkId,
			RequestId: atomic.AddUint64(&ck.requestId, 1),
		},
	}
	for i := 0; ; i = (i + 1) % len(ck.servers) {
		reply := &PutAppendReply{}
		DPrintf("clerkId: %v, requestId: %v, PutAppend args: %v to %v", ck.clerkId, ck.requestId, args, i)
		if ok := ck.servers[i].Call("RaftKV.PutAppend", args, reply); ok && !reply.Meta.WrongLeader {
			DPrintf("clerkId: %v, requestId: %v, PutAppend reply: %v", ck.clerkId, ck.requestId, reply)
			return
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, OP_PUT)
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, OP_APPEND)
}

package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"labrpc"
)

type RaftState int

const (
	RAFT_FOLLOWER RaftState = iota
	RAFT_CANDIDATE
	RAFT_LEADER
)

const (
	HEARTBEAT_TIMEOUT       = time.Millisecond * 200
	ELECTION_MIN_TIMEOUT_MS = 400
	ELECTION_MAX_TIMEOUT_MS = 800
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type LogEntry struct {
	Command interface{}
	Index   int
	Term    int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	isRunning     bool
	votesCount    int
	electionEnds  chan bool
	electionPing  chan int
	heartbeatPing chan int
	applyPing     chan bool
	state         RaftState
	applyCh       chan ApplyMsg

	currentTerm int        // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int        // candidateId that received vote in current term (or null if none)
	log         []LogEntry // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
	commitIndex int        // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int        // index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	nextIndex   []int      // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex  []int      // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
}

func (rf *Raft) getInfo() string {
	return fmt.Sprintf("me: %v, state: %v, currentTerm: %v, votesCount: %v, votedFor: %v, log: %v, commitIndex: %v, lastApplied: %v, nextIndex: %v, matchIndex: %v",
		rf.me, rf.state, rf.currentTerm, rf.votesCount, rf.votedFor, rf.log, rf.commitIndex, rf.lastApplied, rf.nextIndex, rf.matchIndex)
}

func (rf *Raft) getLastEntry() LogEntry {
	return rf.log[len(rf.log)-1]
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.currentTerm, rf.state == RAFT_LEADER
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.commitIndex)
	e.Encode(rf.lastApplied)
	e.Encode(rf.log)
	rf.persister.SaveRaftState(w.Bytes())
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.commitIndex)
	d.Decode(&rf.lastApplied)
	d.Decode(&rf.log)
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate’s term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry (§5.4)
	LastLogTerm  int // term of candidate’s last log entry (§5.4)
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		return
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.switchTo(RAFT_FOLLOWER)
	}
	if rf.votedFor >= 0 && rf.votedFor != args.CandidateId {
		return
	}
	lastEntry := rf.getLastEntry()
	if args.LastLogTerm > lastEntry.Term || (args.LastLogTerm == lastEntry.Term && args.LastLogIndex >= lastEntry.Index) {
		rf.votedFor = args.CandidateId
		rf.votesCount = 0
		reply.VoteGranted = true
		go func() { rf.electionPing <- args.CandidateId }()
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	if !rf.peers[server].Call("Raft.RequestVote", args, reply) {
		return false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	DPrintf("%s, sent request vote to %v, args: %v, reply: %v", rf.getInfo(), server, args, reply)
	if args.Term != rf.currentTerm || rf.state != RAFT_CANDIDATE {
		return true
	}
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.switchTo(RAFT_FOLLOWER)
		return true
	}
	if reply.VoteGranted {
		rf.votesCount++
		if rf.votesCount > len(rf.peers)/2 {
			rf.electionEnds <- true
			rf.switchTo(RAFT_LEADER)
		}
	}
	return true
}

func (rf *Raft) broadcastRequestVote() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != RAFT_CANDIDATE {
		return
	}
	DPrintf("%s, start to send request vote to all", rf.getInfo())
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastEntry().Index,
		LastLogTerm:  rf.getLastEntry().Term,
	}
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int) {
			rf.sendRequestVote(server, args, &RequestVoteReply{})
		}(server)
	}
}

type AppendEntriesArgs struct {
	Term         int        // leader’s term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat;may send more than one for efficiency)
	LeaderCommit int        // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		return
	}

	go func() { rf.heartbeatPing <- args.LeaderId }()

	rf.switchTo(RAFT_FOLLOWER)
	rf.currentTerm = args.Term
	rf.votedFor = args.LeaderId
	rf.votesCount = 0

	match := len(rf.log) - 1
	for ; match >= 0; match-- {
		if rf.log[match].Index == args.PrevLogIndex && rf.log[match].Term == args.PrevLogTerm {
			break
		}
	}
	if match < 0 {
		return
	}

	reply.Success = true
	rf.log = append(rf.log[:match+1], args.Entries...)
	if args.LeaderCommit >= len(rf.log) {
		rf.commitIndex = len(rf.log) - 1
	} else {
		rf.commitIndex = args.LeaderCommit
	}
	rf.applyPing <- true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	if !rf.peers[server].Call("Raft.AppendEntries", args, reply) {
		return false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if rf.state != RAFT_LEADER || args.Term != rf.currentTerm {
		return true
	}
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.switchTo(RAFT_FOLLOWER)
		return true
	}
	if len(args.Entries) == 0 {
		return true
	}
	if reply.Success {
		rf.nextIndex[server] = args.Entries[len(args.Entries)-1].Index + 1
	} else {
		rf.nextIndex[server]--
	}
	rf.matchIndex[server] = rf.nextIndex[server] - 1
	return true
}

func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if rf.state != RAFT_LEADER {
		return
	}

	DPrintf("%s, start to send heartbeat to all", rf.getInfo())

	for i := len(rf.log) - 1; i > rf.commitIndex && rf.log[i].Term == rf.currentTerm; i-- {
		commitCount := 1
		for j := 0; j < len(rf.peers); j++ {
			if j == rf.me {
				continue
			}
			if rf.matchIndex[j] >= i {
				commitCount++
			}
		}
		if commitCount > len(rf.peers)/2 {
			rf.commitIndex = i
			rf.applyPing <- true
			break
		}
	}

	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		prevEntry := rf.log[rf.matchIndex[server]]
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevEntry.Index,
			PrevLogTerm:  prevEntry.Term,
			Entries:      make([]LogEntry, len(rf.log)-(prevEntry.Index+1)),
			LeaderCommit: rf.commitIndex,
		}
		copy(args.Entries, rf.log[prevEntry.Index+1:])
		go func(server int, args *AppendEntriesArgs) {
			rf.sendAppendEntries(server, args, &AppendEntriesReply{})
		}(server, args)
	}
}

func (rf *Raft) electionTimeout() time.Duration {
	timeout := rand.Intn(ELECTION_MAX_TIMEOUT_MS-ELECTION_MIN_TIMEOUT_MS) + ELECTION_MIN_TIMEOUT_MS
	return time.Duration(timeout) * time.Millisecond
}

func (rf *Raft) switchTo(state RaftState) {
	DPrintf("%s, state from %v -> %v", rf.getInfo(), rf.state, state)
	rf.state = state
	switch state {
	case RAFT_FOLLOWER:
		rf.votedFor = -1
		rf.votesCount = 0
	case RAFT_CANDIDATE:
		rf.votedFor = rf.me
		rf.currentTerm++
		rf.votesCount = 1
		rf.electionEnds = make(chan bool)
	case RAFT_LEADER:
		rf.matchIndex = make([]int, len(rf.peers))
		rf.nextIndex = make([]int, len(rf.peers))
		for i := range rf.peers {
			rf.nextIndex[i] = rf.getLastEntry().Index + 1
			rf.matchIndex[i] = 0
		}
	}
}

func (rf *Raft) electionLoop() {
	rf.mu.Lock()
	rf.switchTo(RAFT_FOLLOWER)
	rf.persist()
	rf.mu.Unlock()
	for {
		rf.mu.RLock()
		state := rf.state
		running := rf.isRunning
		rf.mu.RUnlock()
		if !running {
			break
		}
		switch state {
		case RAFT_FOLLOWER:
			select {
			case from := <-rf.heartbeatPing:
				rf.mu.RLock()
				DPrintf("%s, received heartbeat ping from %v", rf.getInfo(), from)
				rf.mu.RUnlock()
			case from := <-rf.electionPing:
				rf.mu.RLock()
				DPrintf("%s, received election ping from %v", rf.getInfo(), from)
				rf.mu.RUnlock()
			case <-time.After(rf.electionTimeout()):
				rf.mu.Lock()
				DPrintf("%s, follower start to election", rf.getInfo())
				rf.state = RAFT_CANDIDATE
				rf.mu.Unlock()
			}
		case RAFT_CANDIDATE:
			rf.mu.Lock()
			rf.switchTo(RAFT_CANDIDATE)
			rf.persist()
			rf.mu.Unlock()
			rf.broadcastRequestVote()
			select {
			case <-rf.electionEnds:
			case <-time.After(rf.electionTimeout()):
			}
		case RAFT_LEADER:
			rf.broadcastAppendEntries()
			time.Sleep(HEARTBEAT_TIMEOUT)
		}
	}
}

func (rf *Raft) applyLoop() {
	for {
		<-rf.applyPing
		rf.mu.Lock()
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			msg := ApplyMsg{
				Index:   rf.log[rf.lastApplied].Index,
				Command: rf.log[rf.lastApplied].Command,
			}
			DPrintf("%s, apply %v", rf.getInfo(), msg)
			rf.applyCh <- msg
			rf.persist()
		}
		rf.mu.Unlock()
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	index := -1
	term, isLeader := rf.GetState()
	if isLeader {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		defer rf.persist()

		DPrintf("%s, start to append %v", rf.getInfo())

		index = len(rf.log)
		rf.log = append(rf.log, LogEntry{
			Command: command,
			Term:    rf.currentTerm,
			Index:   index,
		})
	}
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.isRunning = false
	DPrintf("%s, killed!!!", rf.getInfo())
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rand.Seed(time.Now().UnixNano())
	rf.isRunning = true
	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{
		Term:  0,
		Index: 0,
	})
	rf.currentTerm = 0
	rf.lastApplied = 0
	rf.commitIndex = 0
	rf.applyCh = applyCh

	rf.heartbeatPing = make(chan int)
	rf.electionPing = make(chan int)
	rf.applyPing = make(chan bool)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.electionLoop()
	go rf.applyLoop()

	return rf
}

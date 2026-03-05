package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	"fmt"
	"strconv"

	//	"bytes"
	"math/rand"
	"sync"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

// Raft A Go object implementing a single Raft peer.
type peerstate int

const (
	PEER_FOLLOWER = iota
	PEER_CANDIDATE
	PEER_LEADER
)

type Raft struct {
	mu        DebugMutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm int
	state       peerstate
	todo        chan raftapi.ApplyMsg
	lastContact time.Time

	wait int

	granted int

	votedFor int
}

const (
	HEART_RACE = 100 * time.Millisecond
)

type DebugMutex struct {
	mu   sync.Mutex
	name string
}

func (m *DebugMutex) Lock() {
	m.mu.Lock()
}

func (m *DebugMutex) Unlock() {
	m.mu.Unlock()
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()         // TODO: Lock
	defer rf.mu.Unlock() // TODO: Unlock
	return rf.currentTerm, rf.state == PEER_LEADER
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// PersistBytes how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()         // TODO: Lock
	defer rf.mu.Unlock() // TODO: Unlock
	return rf.persister.RaftStateSize()
}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// RequestVoteArgs example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Whoami int
	Term   int
}

// RequestVoteReply example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Success     bool
	VoteGranted bool
	Term        int
}

// RequestVote example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock() // TODO: Lock
	DPrintf("[RequestVote] %v -> %v\n", args.Whoami, rf.me)
	defer rf.mu.Unlock() // TODO: Unlock
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.resetAlarm()
		DPrintf("[RequestVote] failed reason -> term is old\n")
		return
	}
	reply.Success = true
	if args.Term > rf.currentTerm {
		rf.updateTerm(args.Term)
		rf.setState(PEER_FOLLOWER)
	}
	if rf.votedFor == -1 {
		reply.VoteGranted = true
		rf.votedFor = args.Whoami
	} else {
		DPrintf("[RequestVote] failed reason -> cannot grant\n")
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm
	rf.resetAlarm()
}

type AppendEntriesArgs struct {
	HeartBeaten bool
	Term        int
	Who         int
}
type AppendEntriesReply struct {
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()         // TODO: Lock
	defer rf.mu.Unlock() // TODO: Unlock
	if args.HeartBeaten && args.Term >= rf.currentTerm {
		if args.Term > rf.currentTerm {
			rf.updateTerm(args.Term)
		}
		rf.setState(PEER_FOLLOWER)
		rf.resetAlarm()
		reply.Success = true
		return
	}
	rf.resetAlarm()
}

// helper function
func (rf *Raft) sendAppendEntries(who int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[who].Call("Raft.AppendEntries", args, reply)
	return ok
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// Start
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

func (rf *Raft) raftLoop() {
	for {
		rf.mu.Lock() // TODO: Lock
		state := rf.state
		whoami := rf.me
		rf.mu.Unlock() // TODO: Unlock
		switch state {
		case PEER_FOLLOWER:
			DPrintf("[FOLLOWER] %v\n", whoami)
			rf.follower()
		case PEER_CANDIDATE:
			DPrintf("[CANDIDATE] %v\n", whoami)
			rf.candidate()
		case PEER_LEADER:
			DPrintf("[LEADAER] %v\n", whoami)
			rf.leader()
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) follower() {
	if !rf.checkState(PEER_FOLLOWER) {
		return
	}
	select {
	case td := <-rf.todo:
		fmt.Println(td)
	default:
		rf.mu.Lock() // TODO: Lock
		to := rf.lastContact
		wt := rf.wait
		rf.mu.Unlock() // TODO: Unlock

		if time.Since(to) > time.Duration(wt)*time.Millisecond {
			rf.mu.Lock() // TODO: Lock
			rf.resetAlarm()
			if rf.state != PEER_FOLLOWER {
				rf.mu.Unlock() // TODO: Unlock
				return
			}
			rf.setState(PEER_CANDIDATE)
			rf.currentTerm++
			rf.granted = 1
			rf.votedFor = rf.me
			curTerm := rf.currentTerm
			whoami := rf.me
			rf.mu.Unlock() // TODO: Unlock

			DPrintf("[FOLLOWER] %v request for votes. term = %v\n", whoami, curTerm)
			rf.requestVoteForme(whoami, curTerm)
			return
		}
	}
}

func (rf *Raft) requestVoteForme(who int, term int) {
	for i := 0; i < len(rf.peers); i++ {
		if i == who {
			continue
		}
		go func() {
			args := RequestVoteArgs{Whoami: who, Term: term}
			reply := RequestVoteReply{}
			DPrintf("[sendRequestVote] %v -> %v, term = %v\n", who, i, term)
			ok := rf.sendRequestVote(i, &args, &reply)
			if ok {
				DPrintf("reply = %v\n", reply)
				rf.mu.Lock() // TODO: Lock
				if rf.state != PEER_CANDIDATE || reply.Term < rf.currentTerm {
					rf.mu.Unlock()
					return
				}
				if reply.Success == false && rf.currentTerm < reply.Term {
					rf.updateTerm(reply.Term)
					rf.resetAlarm()
					rf.mu.Unlock()
					return
				}
				if reply.Success == true && reply.VoteGranted == true {
					rf.granted++
					// 5 -> 3; 6 -> 4
					if rf.granted >= len(rf.peers)/2+1 {
						DPrintf("[FOLLOWER] %v get vote from %v\n", rf.me, i)
						rf.setState(PEER_LEADER)
						rf.resetAlarm()
						rf.mu.Unlock() // TODO: Unlock
						//rf.declareLeader()
						return
					}
					rf.mu.Unlock() // TODO: Unlock
				} else {
					rf.mu.Unlock()
				}
			}
		}()
	}
}

func (rf *Raft) declareLeader() {
	rf.mu.Lock() // TODO: Lock
	whoami := rf.me
	curTerm := rf.currentTerm
	DPrintf("[LEADER] peer = %v, term = %v\n", whoami, curTerm)
	rf.mu.Unlock() // TODO: Unlock
	rf.heartBeaten(whoami, curTerm)
}

func (rf *Raft) candidate() {
	if !rf.checkState(PEER_CANDIDATE) {
		return
	}

	// election out: back to follower
	rf.mu.Lock() // TODO: Lock
	to := rf.lastContact
	wt := rf.wait
	rf.mu.Unlock() // TODO: Unlock
	if time.Since(to) > time.Duration(wt)*time.Millisecond {
		rf.setState(PEER_FOLLOWER)
	}
}

func (rf *Raft) leader() {
	if !rf.checkState(PEER_LEADER) {
		return
	}
	rf.mu.Lock() // TODO: Lock
	whoami := rf.me
	curTerm := rf.currentTerm
	t := rf.lastContact
	rf.mu.Unlock() // TODO: Unlock
	if time.Since(t) > HEART_RACE {
		rf.heartBeaten(whoami, curTerm)
	}
}

func (rf *Raft) heartBeaten(whoami int, curTerm int) {
	rf.mu.Lock() // TODO: Lock
	rf.resetAlarm()
	rf.mu.Unlock() // TODO: Unlock

	for i := 0; i < len(rf.peers); i++ {
		if i == whoami {
			continue
		}
		msg := AppendEntriesArgs{HeartBeaten: true, Term: curTerm, Who: whoami}
		reply := AppendEntriesReply{}
		rf.mu.Lock()
		if rf.state != PEER_LEADER {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		go func() {
			rf.sendAppendEntries(i, &msg, &reply)
		}()
	}
}

func (rf *Raft) resetAlarm() {
	rf.lastContact = time.Now()
	rf.wait = 400 + rand.Intn(300)
}

func (rf *Raft) checkState(expect peerstate) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state == expect
}

func (rf *Raft) updateTerm(target int) {
	rf.currentTerm = target
	rf.votedFor = -1
	rf.granted = 0
}

func (rf *Raft) setState(s peerstate) {
	rf.state = s
}

// Make
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.state = PEER_FOLLOWER
	rf.todo = applyCh
	rf.resetAlarm()
	rf.votedFor = -1
	rf.mu.name = strconv.Itoa(me)
	rand.Seed(time.Now().UnixNano())

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start raftLoop goroutine to start elections
	go rf.raftLoop()

	return rf
}

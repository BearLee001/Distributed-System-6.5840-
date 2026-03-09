package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	"fmt"
	//	"bytes"
	"math/rand"
	"strconv"
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

	term  int
	state peerstate

	lastContact time.Time
	wait        int
	granted     int
	votedFor    int
	leaderIndex int

	out          chan raftapi.ApplyMsg
	todo         chan interface{}
	logs         []LogEntry // len == 1 or 0
	lastLogIndex int
	lastLogTerm  int

	// leader
	// expected replicated log index, if lastLogIndex < nextIndex, do nothing
	nextIndex  []int
	matchIndex []int

	commitIndex  int
	appliedIndex int
	applyCond    *sync.Cond
}

type LogEntry struct {
	Command interface{}
	Term    int
	Index   int
}

func (log *LogEntry) info() string {
	return fmt.Sprintf("{command = %v, term = %v, Index = %v}", log.Command, log.Term, log.Index)
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

// GetState return term and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.term, rf.state == PEER_LEADER
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
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

	LastLogIndex int
	LastLogTerm  int
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.term {
		reply.Term = rf.term
		reply.Success = false
		rf.resetAlarm()
		return
	}
	if args.Term > rf.term {
		rf.updateTerm(args.Term)
		rf.setState(PEER_FOLLOWER)
	}
	// Note: 5.4.1 safety
	if rf.fight(args.LastLogIndex, args.LastLogTerm) {
		reply.Success = false
		rf.resetAlarm()
		return
	}
	reply.Success = true
	if rf.votedFor == -1 {
		reply.VoteGranted = true
		rf.votedFor = args.Whoami
	} else {
		reply.VoteGranted = false
	}
	reply.Term = rf.term
	rf.resetAlarm()
}

func (rf *Raft) fight(lastLogIndex int, lastLogTerm int) bool {
	return rf.lastLogTerm > lastLogTerm ||
		(rf.lastLogTerm == lastLogTerm && rf.lastLogIndex > lastLogIndex)
}

type AppendEntriesArgs struct {
	Entries []LogEntry
	PIndex  int
	PTerm   int
	Leader  int
	Term    int

	LeaderCommit int
}
type AppendEntriesResponse struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesResponse) {
	rf.mu.Lock()
	DPrintf("[RPC] AppendEntries %v -> %v, logs = %v\n", args.Leader, rf.me, args.Entries)
	defer rf.mu.Unlock()
	if args.Term < rf.term {
		reply.Success = false
		reply.Term = rf.term
		return
	}
	rf.setState(PEER_FOLLOWER)
	if args.Term > rf.term {
		rf.updateTerm(args.Term)
	}
	// Append log
	rf.resetAlarm()
	reply.Term = rf.term
	reply.Success = rf.tryUpdateLogs(args.Entries, args.PIndex, args.PTerm, args.LeaderCommit)
}

func (rf *Raft) tryUpdateLogs(ls []LogEntry, pIndex int, pTerm int, leaderCommit int) bool {
	// [AppendEntries] -> [tryUpdateLogs] Note: [AppendEntries] is locked
	DPrintf("[tryUpdateLogs] entries = %v, pIndex = %v, pTerm = %v, leaderCommit = %v\n", ls, pIndex, pTerm, leaderCommit)
	if !rf.checkPre(pIndex, pTerm) {
		return false
	}
	if ls == nil {
		//DPrintf("[tryUpdateLogs] heartbeat!!!\n")
	}
	rf.logs = rf.logs[:pIndex+1]
	rf.logs = append(rf.logs, ls...)
	rf.lastLogIndex = len(rf.logs) - 1
	rf.lastLogTerm = rf.logs[rf.lastLogIndex].Term
	if rf.commitIndex < leaderCommit {
		rf.commitIndex = min(rf.lastLogIndex, leaderCommit)
		DPrintf("[%v] set commitIndex -> %v\n", rf.me, rf.commitIndex)
		rf.applyCond.Signal()
	}
	return true
}

func (rf *Raft) checkPre(pIndex int, pTerm int) bool {
	DPrintf("[%v] check: logs = %v\n", rf.me, rf.logs)
	if pIndex == -1 {
		return true
	}
	if 0 <= pIndex && pIndex < len(rf.logs) {
		return rf.logs[pIndex].Term == pTerm
	}
	return false
}

// helper function
func (rf *Raft) sendAppendEntries(who int, args *AppendEntriesArgs, reply *AppendEntriesResponse) bool {
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[START] command = %v, peer = %v\n", command, rf.me)
	isLeader := rf.state == PEER_LEADER
	if !isLeader {
		return -1, rf.term, false
	}
	term := rf.term
	log := LogEntry{Command: command, Term: rf.term, Index: rf.lastLogIndex + 1}
	rf.logs = append(rf.logs, log)
	rf.lastLogIndex = len(rf.logs) - 1
	rf.lastLogTerm = rf.logs[rf.lastLogIndex].Term
	rf.matchIndex[rf.me] = rf.lastLogIndex
	rf.nextIndex[rf.me] = rf.lastLogIndex + 1
	return rf.lastLogIndex, term, isLeader
}

func (rf *Raft) raftLoop() {
	for {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()

		switch state {
		case PEER_FOLLOWER:
			rf.follower()
		case PEER_CANDIDATE:
			rf.candidate()
		case PEER_LEADER:
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
	case _ = <-rf.todo:
		return
		//panic("[FOLLOWER] should never receive message from user.")
	default:
		rf.mu.Lock()
		to := rf.lastContact
		wt := rf.wait
		rf.mu.Unlock()

		if time.Since(to) > time.Duration(wt)*time.Millisecond {
			rf.mu.Lock()
			rf.resetAlarm()
			if rf.state != PEER_FOLLOWER {
				rf.mu.Unlock()
				return
			}
			rf.setState(PEER_CANDIDATE)
			rf.term++
			rf.granted = 1
			rf.votedFor = rf.me
			curTerm := rf.term
			whoami := rf.me
			lastIndex := rf.lastLogIndex
			lastTerm := rf.lastLogTerm
			rf.mu.Unlock()

			rf.requestVoteForme(whoami, curTerm, lastIndex, lastTerm)
			return
		}
	}
}

func (rf *Raft) requestVoteForme(who int, term int, lastLogIndex int, lastLogTerm int) {
	for i := 0; i < len(rf.peers); i++ {
		if i == who {
			continue
		}
		peer := i
		go func() {
			to := peer
			args := RequestVoteArgs{Whoami: who, Term: term, LastLogIndex: lastLogIndex, LastLogTerm: lastLogTerm}
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(to, &args, &reply)
			if ok {
				rf.mu.Lock()
				if rf.state != PEER_CANDIDATE || reply.Term < rf.term {
					rf.mu.Unlock()
					return
				}
				if reply.Success == false && rf.term < reply.Term {
					rf.updateTerm(reply.Term)
					rf.resetAlarm()
					rf.mu.Unlock()
					return
				}
				if reply.Success == true && reply.VoteGranted == true {
					rf.granted++
					// 5 -> 3; 6 -> 4
					if rf.granted >= len(rf.peers)/2+1 {
						rf.setState(PEER_LEADER)
						rf.resetNextAndMatchs()
						rf.resetAlarm()
						rf.mu.Unlock()
						return
					}
					rf.mu.Unlock()
				} else {
					rf.mu.Unlock()
				}
			}
		}()
	}
}

func (rf *Raft) resetNextAndMatchs() {
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.lastLogIndex + 1
		rf.matchIndex[i] = 0

		if i == rf.me {
			DPrintf("[%v] logs = %v, lastLogIndex = %v\n", rf.me, rf.logs, rf.lastLogIndex)
			rf.matchIndex[i] = rf.lastLogIndex
			rf.nextIndex[i] = rf.lastLogIndex + 1
		}
	}
}

func (rf *Raft) declareLeader() {
	rf.mu.Lock()
	whoami := rf.me
	curTerm := rf.term
	DPrintf("[LEADER] peer = %v, term = %v\n", whoami, curTerm)
	rf.mu.Unlock()
	rf.broadcast()
}

func (rf *Raft) candidate() {
	if !rf.checkState(PEER_CANDIDATE) {
		return
	}

	// election out: back to follower
	rf.mu.Lock()
	to := rf.lastContact
	wt := rf.wait
	if time.Since(to) > time.Duration(wt)*time.Millisecond {
		rf.setState(PEER_FOLLOWER)
	}
	rf.mu.Unlock()
}

func (rf *Raft) leader() {
	if !rf.checkState(PEER_LEADER) {
		return
	}
	rf.mu.Lock()
	t := rf.lastContact
	rf.mu.Unlock()
	if time.Since(t) > HEART_RACE {
		rf.broadcast()
	}
}

func (rf *Raft) appendLogs(msg interface{}) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log := LogEntry{Command: msg, Term: rf.term, Index: rf.lastLogIndex + 1}
	DPrintf("[LEADER] append log = %v\n", log)
	rf.logs = append(rf.logs, log)
	rf.lastLogIndex = len(rf.logs) - 1
	rf.lastLogTerm = rf.logs[rf.lastLogIndex].Term
	rf.matchIndex[rf.me] = rf.lastLogIndex
	rf.nextIndex[rf.me] = rf.lastLogIndex + 1
	DPrintf("[appendLogs-%v] logs = %v, lastLogIndex = %v, lastLogTerm = %v\n", rf.me, rf.logs, rf.lastLogIndex, rf.lastLogTerm)
}

func (rf *Raft) broadcastAppend() {
	rf.mu.Lock()
	leader := rf.me
	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i == leader {
			continue
		}
		rf.mu.Lock()
		if rf.state != PEER_LEADER {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		who := i
		go func() {
			peer := who
			rf.replicate(peer)
		}()
	}
}

func (rf *Raft) replicate(to int) {
	// Debug =============================== Debug
	//tag := time.Now().UnixNano()
	//rf.mu.Lock()
	//DPrintf("[REPLICATE-%v] %v -> %v\n", tag, rf.me, to)
	//defer DPrintf("[REPLICATE-%v] exit", tag)
	//rf.mu.Unlock()
	// Debug =============================== Debug

	for {
		if !rf.checkState(PEER_LEADER) {
			return
		}
		rf.mu.Lock()
		nextIdx := rf.nextIndex[to]
		pIndex := nextIdx - 1
		pTerm := rf.logs[pIndex].Term
		entries := append([]LogEntry(nil), rf.logs[nextIdx:]...)
		term := rf.term
		leader := rf.me
		leaderCommit := rf.commitIndex
		//DPrintf("[REPLICATE] send log entries: %v to %v\n", nextIdx, len(rf.logs)-1)
		rf.mu.Unlock()
		msg := AppendEntriesArgs{
			Entries:      entries,
			PIndex:       pIndex,
			PTerm:        pTerm,
			Term:         term,
			Leader:       leader,
			LeaderCommit: leaderCommit,
		}
		reply := AppendEntriesResponse{}
		ok := rf.sendAppendEntries(to, &msg, &reply)
		if ok {
			if reply.Success {
				// succeed to replicate the log
				rf.mu.Lock()
				DPrintf("[SUCCESS] %v replicate to peer %v, replicated logs = %v, term = %v\n", rf.me, to, entries, term)

				// Note: Not rf.nextIndex[to] = rf.lastLogIndex!!!
				// Note: leader send append messages ---> succeed -> leader receive a command from outside ---> here
				rf.matchIndex[to] = pIndex + len(entries)
				rf.nextIndex[to] = rf.matchIndex[to] + 1
				rf.mu.Unlock()

				rf.tryCommit()
				return
			}
			rf.mu.Lock()
			if reply.Term > rf.term {
				DPrintf("[OUTDATED] %v back to follower\n", rf.me)
				rf.state = PEER_FOLLOWER
				rf.resetAlarm()
				rf.updateTerm(reply.Term)
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()

			// backtrace the find the log if is still leader
			rf.mu.Lock()
			if rf.state != PEER_LEADER {
				rf.mu.Unlock()
				return
			}
			DPrintf("[REPLICATE] backtrace\n")
			rf.nextIndex[to]--
			rf.mu.Unlock()
		}
		// No reply, don't backtrace
	}
}

func (rf *Raft) tryCommit() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	start := rf.commitIndex + 1
	end := rf.lastLogIndex
	var result = rf.commitIndex
	for i := start; i <= end; i++ { // TODO: Is binary search justified here?
		if rf.checkCommitIndex(i) {
			result = i
		}
	}
	rf.commitIndex = result
	rf.applyCond.Signal()
}

func (rf *Raft) checkCommitIndex(cIndex int) bool {
	votes := 0
	for i := 0; i < len(rf.peers); i++ {
		if rf.matchIndex[i] >= cIndex {
			votes++
		}
	}
	return votes >= len(rf.peers)/2+1 && rf.logs[cIndex].Term == rf.term
}

func (rf *Raft) broadcast() {
	rf.mu.Lock()
	rf.resetAlarm()
	rf.mu.Unlock()

	rf.broadcastAppend()
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
	rf.term = target
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
	rf.initRaftPeer(peers, me, persister, applyCh)
	rand.Seed(time.Now().UnixNano())

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start raftLoop goroutine to start elections
	go rf.raftLoop()
	go rf.applyLogs()

	return rf
}

func (rf *Raft) initRaftPeer(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) {
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.state = PEER_FOLLOWER
	rf.out = applyCh
	rf.todo = make(chan interface{})
	rf.resetAlarm()
	rf.votedFor = -1
	rf.mu.name = strconv.Itoa(me)

	rf.lastLogTerm = 0
	rf.lastLogIndex = 0

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	for i := 0; i < len(peers); i++ {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = 1
	}
	rf.commitIndex = 0
	rf.appliedIndex = 0
	if len(rf.logs) != 0 {
		panic("raft should not have logs when initializing!!!")
	}
	rf.logs = append(rf.logs, LogEntry{Command: -1, Term: -1, Index: 0})

	rf.applyCond = sync.NewCond(&rf.mu)
}

func (rf *Raft) applyLogs() {
	for {
		rf.mu.Lock()
		for rf.appliedIndex >= rf.commitIndex {
			rf.applyCond.Wait()
		}

		msgs := make([]raftapi.ApplyMsg, 0, rf.commitIndex-rf.appliedIndex)
		for i := rf.appliedIndex + 1; i <= rf.commitIndex; i++ {
			log := rf.logs[i]
			msgs = append(msgs, raftapi.ApplyMsg{CommandValid: true, Command: log.Command, CommandIndex: log.Index})
		}
		rf.appliedIndex = rf.commitIndex
		DPrintf("%v -> %v\n", rf.me, msgs)
		for i := 0; i <= rf.commitIndex; i++ {
			DPrintf("[%v] committed logs = %v\n", rf.me, rf.logs[i])
		}
		rf.mu.Unlock()
		for _, msg := range msgs {
			rf.out <- msg
		}
	}
}

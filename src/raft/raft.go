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
	//	"bytes"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term    int
	Command interface{}
	Index   int
}

type ServerRole int

const (
	Leader ServerRole = iota
	Follower
	Candidate
)

// milliseconds
const (
	TIMEOUT_ELECTION_MIN = 160
	TIMEOUT_ELECTION_MAX = 250
	HEARTBEAT_INTERVAL   = 50
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role        ServerRole
	currentTerm int
	votedFor    int
	log         []LogEntry

	// volatile
	// common
	commitIndex int
	lastApplied int

	// leader
	nextIndex  []int
	matchIndex []int

	// custom
	lastHeartbeat   time.Time
	electionTimeout time.Duration

	applyCh chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return rf.currentTerm, rf.isLeader()
}

func (rf *Raft) isLeader() bool {
	return rf.role == Leader
}

func (rf *Raft) resetElectionTimer() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Debug(dLog, "S%d Reset Election Timeout", rf.me)
	// randomize election timeout
	rf.electionTimeout = time.Duration(rand.Intn(TIMEOUT_ELECTION_MAX-TIMEOUT_ELECTION_MIN)+TIMEOUT_ELECTION_MIN) * time.Millisecond
	rf.lastHeartbeat = time.Now()
}

func (rf *Raft) resetHeartbeatTimer() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.lastHeartbeat = time.Now()
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
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
	// Your code here (2C).
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	Debug(dVote, "S%d Received request vote from S%d", rf.me, args.CandidateId)
	// Your code here (2A, 2B).
	rf.resetElectionTimer()
	Debug(dVote, "S%d Resetted Election Timeout", rf.me)
	if args.Term < rf.currentTerm {
		Debug(dVote, "S%d invalid term (args %d < cterm %d)", rf.me, args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && (args.LastLogTerm >= rf.getLastLogTerm() && args.LastLogIndex >= rf.getLastLogIndex()) {
		Debug(dVote, "S%d Voted for S%d", rf.me, args.CandidateId)
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		reply.Term = args.Term
		reply.VoteGranted = true
		return
	}

	Debug(dVote, "S%d Rejected vote request of S%d. State: votedFor = %d currentTerm = %d", rf.me, args.CandidateId, rf.votedFor, rf.currentTerm)
	reply.Term = args.Term
	reply.VoteGranted = false
}

// example code to send a RequestVote RPC to a server.  // server is the index of the target server in rf.peers[].
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
	Debug(dVote, "S%d Sending request vote to %d", rf.me, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.resetElectionTimer()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Debug(dTrace, "S%d Received append entries from S%d\n", rf.me, args.LeaderId)
	if args.Term < rf.currentTerm {
		Debug(dTerm, "S%d Invalid term received from S%d - (%d < %d)\n", rf.me, args.LeaderId, args.Term, rf.currentTerm)
		reply.Success = false
		reply.Term = rf.currentTerm
	}

	if args.Term >= rf.currentTerm {
		Debug(dTerm, "S%d Recognize leader as S%d\n", rf.me, args.LeaderId)
		rf.currentTerm = args.Term
		rf.role = Follower
		rf.votedFor = -1
		reply.Success = true
		reply.Term = args.Term
	}
}

func (rf *Raft) checkRole(role ServerRole) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.role == role
}

func (rf *Raft) ticker() {
	rf.resetElectionTimer()
	rf.resetHeartbeatTimer()
	for !rf.killed() {
		// Your code here (2A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		electionTimedout := time.Since(rf.lastHeartbeat) >= rf.electionTimeout
		heartbeatTimedout := time.Since(rf.lastHeartbeat) >= HEARTBEAT_INTERVAL*time.Millisecond
		rf.mu.Unlock()
		if electionTimedout {
			if !rf.checkRole(Leader) {
				rf.startElection()
			}
		} else if heartbeatTimedout {
			if rf.role == Leader {
				rf.sendHeartbeat()
			}
		}
	}
}

func (rf *Raft) startElection() {
	Debug(dVote, "S%d Start election", rf.me)
	if rf.checkRole(Follower) {
		rf.role = Candidate
	}
	var numberOfVote int
	rf.mu.Lock()
	// steps to start election
	// 1. increase term
	rf.currentTerm++

	// 2. vote for self
	numberOfVote = 1
	rf.votedFor = rf.me
	rf.persist()
	rf.mu.Unlock()

	// 3. send RequestVote RPC
	var wg sync.WaitGroup
	for server := range rf.peers {
		if server == rf.me {
			continue
		}

		wg.Add(1)
		go func(server int) {
			defer wg.Done()
			var reply RequestVoteReply
			ok := rf.sendRequestVote(server, &RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: rf.getLastLogIndex(),
				LastLogTerm:  rf.getLastLogTerm(),
			}, &reply)

			if !ok {
				Debug(dError, "S%d Failed to send RequestVote to S%d\n", rf.me, server)
				return
			}
			if rf.checkRole(Leader) {
				return
			}
			if reply.Term > rf.currentTerm && !reply.VoteGranted {
				Debug(dLog, "S%d Rejected vote from S%d - (%d > %d)\n", rf.me, server, reply.Term, rf.currentTerm)
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.role = Follower
				return
			}

			if reply.VoteGranted && reply.Term == rf.currentTerm {
				Debug(dVote, "S%d Received granted vote from S%d\n", rf.me, server)
				numberOfVote++

				if rf.checkRole(Candidate) {
					if numberOfVote >= len(rf.peers)/2 {
						rf.wonElection()
					}
				}
			}

			if !reply.VoteGranted {
				Debug(dVote, "S%d Received rejected vote from S%d. Reply details %v\n", rf.me, server, reply)
			}
		}(server)
	}
	rf.resetElectionTimer()
	go func() {
		wg.Wait()
		if !(numberOfVote >= len(rf.peers)/2) {
			rf.lostElection()
		}
	}()
}

func (rf *Raft) lostElection() {
	Debug(dVote, "S%d Lost election\n", rf.me)
	rf.mu.Lock()
	rf.votedFor = -1
	rf.role = Follower
	rf.mu.Unlock()
	rf.resetElectionTimer()
}

func (rf *Raft) sendHeartbeat() {
	Debug(dLeader, "S%d Send heartbeat\n", rf.me)
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int) {
			reply := AppendEntriesReply{}
			Debug(dLeader, "S%d Send heartbeat to S%d\n", rf.me, server)
			ok := rf.peers[server].Call("Raft.AppendEntries", &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.getLastLogIndex(),
				PrevLogTerm:  rf.getLastLogTerm(),
				Entries:      rf.log,
				LeaderCommit: rf.commitIndex,
			}, &reply)
			Debug(dLeader, "S%d Sent heartbeat to S%d\n", rf.me, server)
			if !ok {
				Debug(dWarn, "S%d Cannot send heartbeat to S%d\n", rf.me, server)
			}
			if !reply.Success {
				if reply.Term > rf.currentTerm {
					Debug(dTrace, "S%d Failed heartbeat to S%d - (reply %d > current %d)\n", rf.me, server, reply.Term, rf.currentTerm)
					rf.currentTerm = reply.Term
					rf.role = Follower
				}
			}
		}(server)
	}

	rf.resetHeartbeatTimer()
	Debug(dTrace, "S%d Sent heartbeat\n", rf.me)
}

func (rf *Raft) wonElection() {
	Debug(dVote, "S%d won election\n", rf.me)
	rf.mu.Lock()
	rf.role = Leader
	// reinitialize volatile state
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.mu.Unlock()
	rf.persist()
	rf.sendHeartbeat()
	rf.resetHeartbeatTimer()
}

func (rf *Raft) getLastLogIndex() int {
	return rf.log[len(rf.log)-1].Index
}

func (rf *Raft) getLastLogTerm() int {
	return rf.log[len(rf.log)-1].Term
}

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
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		currentTerm:     0,
		electionTimeout: time.Since(time.Now()),
		lastHeartbeat:   time.Now(),
		log:             make([]LogEntry, 1),
		applyCh:         applyCh,
		votedFor:        -1,
		role:            Follower,
	}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

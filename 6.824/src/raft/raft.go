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
	// "math"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
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
	Term int64
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int64       // The term me is in
	currentLeader int       // The server which me believes is the leader for currentTerm; -1 means leader is unknown 
	lastHeardFromLeaderTime time.Time  // The most recent time when me is contacted by the leader; ignored if me is the leader
	votedFor map[int64]int  // Term -> the index of the candidate me votes for for the term
	log []LogEntry

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	return int(rf.currentTerm), rf.currentLeader == rf.me
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
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
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


//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int64
	CandidateId int
	LastLogIndex int
	LastLogTerm int64
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int64
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}
	if rf.compareLogUpToDate(args.LastLogIndex, args.LastLogTerm) < 0 {
		// My own log is more up-to-date.
		reply.VoteGranted = false
		return
	}
	if voted_candidate, ok := rf.votedFor[args.Term]; ok && voted_candidate != args.CandidateId {
		// Already voted for someone else.
		reply.VoteGranted = false
		return 
	}
	rf.votedFor[args.Term] = args.CandidateId
	reply.VoteGranted = true
}

type AppendEntriesArgs struct {
	Term int64
	LeaderId int
	// prevLogIdx int
	// prevLogTerm int
	Entries []LogEntry
	// leaderCommit int
}

type AppendEntriesReply struct {
	Term int64
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// TODO-1: for now AppendEntries only handle heartbeats
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		// Stale leader, reject the request
		reply.Success = false
		return
	} 
	if args.Term > rf.currentTerm {
		// A new leader is elected for a future term; accept it
		rf.currentTerm = args.Term
		rf.currentLeader = args.LeaderId
		reply.Success = true
		return
	}
	if rf.currentLeader != -1 && rf.currentLeader != args.LeaderId {
		panic(fmt.Sprintf("Fata bug: the same view %d has two different leaders: %d and %d", 
		args.Term, rf.currentLeader, args.LeaderId))
	}
	if (rf.currentLeader == -1) {
		// Learned about the leader of this term
		rf.currentLeader = args.LeaderId
	}
	rf.lastHeardFromLeaderTime = time.Now()
    reply.Success = true
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}


//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The base time to wait before starting a leader election. The actual wait time
// is randomized; see randomSleep for more detail
const kStartElectionTimeout time.Duration = 1 * time.Second
// The time interval between two consecutive heartbeat messages
const kHeartbeatRoundInterval time.Duration = 100 * time.Millisecond
// The maximum time to wait before aborting the current leader election
const kElectionRoundTimeout time.Duration = 5 * time.Millisecond
// The time interval between sending two rpc requests
const kRpcInterval time.Duration = time.Millisecond


// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	// TODO(jin): in initial start, do not wait for timeout; directly start a new election
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		if (rf.leaderTimesOut()) {
			rf.mu.Lock()
			rf.currentTerm++
			rf.currentLeader = -1
			rf.votedFor[rf.currentTerm] = rf.me
			startTime := time.Now()

			term := rf.currentTerm
			me := rf.me
			lastLogIdx, lastLogTerm := rf.getLastLogIndexAndTerm()
			
			var roundLock sync.Mutex
			voteCount := 1
			yesVoteCount := 1  // Always vote for myself.
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				go func (dest int) {
					var reply RequestVoteReply
					for {			
						if rf.sendRequestVote(dest, &RequestVoteArgs{term, me, lastLogIdx, lastLogTerm}, &reply) {
							break
						}
						time.Sleep(kRpcInterval)
					}
					roundLock.Lock()
					voteCount++
					if reply.VoteGranted {
					    yesVoteCount++
					}
					roundLock.Unlock()
				} (i)
			}
			// Waiting for the election result
			for {
				roundLock.Lock()
				if yesVoteCount >= len(rf.peers) / 2 + 1  || voteCount == len(rf.peers) || time.Now().Sub(startTime) > kElectionRoundTimeout {
					roundLock.Unlock()
					break
				}
				roundLock.Unlock()
				time.Sleep(time.Millisecond)
			}
			electionSucceeds := yesVoteCount >= len(rf.peers) / 2 + 1
			rf.mu.Unlock()
			if electionSucceeds {
				rf.becomeNewLeader()
			}
		}
		randomSleep()
	}
}

func (rf *Raft) periodicHeartbeatIfIsLeader() {
	for rf.killed() == false {
		rf.mu.Lock()
		if (rf.currentLeader == rf.me) {
			// Only send out heartbeats when I think I am the leader.
			term := rf.currentTerm
			leaderId := rf.me
			entries := make([]LogEntry, 0)
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				go func(dest int) {
					// TODO(jin): what should we do with the reply? Should the leader steps down once 
					// receiving a larger term?
					var reply AppendEntriesReply
					// It is ok for this rpc to fail in this round; we will always retry it in the next
					// round.
					rf.sendAppendEntries(dest, &AppendEntriesArgs{term, leaderId, entries}, &reply)
					
				}(i)
			}
		}
		rf.mu.Unlock()
		time.Sleep(kHeartbeatRoundInterval)
	}
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
	rf.currentTerm = 0
	rf.currentLeader = -1
	rf.lastHeardFromLeaderTime = time.Time{}
	rf.votedFor = make(map[int64]int)
	rf.log = make([]LogEntry, 10)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.periodicHeartbeatIfIsLeader()


	return rf
}

func (rf *Raft) leaderTimesOut() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentLeader == rf.me {
		return false
	}
	return rf.currentLeader == -1 || time.Now().Sub(rf.lastHeardFromLeaderTime) < kStartElectionTimeout
}

func (rf *Raft) becomeNewLeader() {
	// TODO: implement
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentLeader = rf.me
}

func randomSleep() {
	s := rand.NewSource(time.Now().UnixNano())
    r := rand.New(s)
	duration := (r.Intn(3) + 1) * int(kStartElectionTimeout)
	time.Sleep(time.Duration(duration));
}

// Not thread-safe
func (rf *Raft) getLastLogIndexAndTerm() (int, int64) {
	var lastLogIdx int;
	var lastLogTerm int64;
	if len(rf.log) == 0 {
		lastLogIdx = -1
		lastLogTerm = -1
	} else {
		lastLogIdx = len(rf.log) - 1
		lastLogTerm = rf.log[lastLogIdx].Term
	}
	return lastLogIdx, lastLogTerm
}

// Returns 1 if my log is strictly more up-to-date, -1 if the given log is, and 0 when
// the two logs are equally up-to-date.
// Not thread-safe. 
func (rf *Raft) compareLogUpToDate(lastLogIndex int, lastLogTerm int64) int {
	myLastLogIndex, myLastLogTerm := rf.getLastLogIndexAndTerm()
	if myLastLogTerm == lastLogTerm && myLastLogIndex == lastLogIndex {
		return 0
	}
	if myLastLogTerm > lastLogTerm || myLastLogTerm == lastLogTerm && myLastLogIndex > lastLogIndex {
		return 1
	}
	return -1
}
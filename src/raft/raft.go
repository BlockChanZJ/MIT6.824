package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)
import "labrpc"

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log Entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogMsg struct {
	term    int
	command interface{}
}

const (
	FOLLOWER  = 0
	CANDIDATE = 1
	LEADER    = 2
)
const (
	ElectionTimeout = 1000 * time.Millisecond
	WaitTimeBase    = 500 * time.Millisecond
	HeartBeatTime   = 150 * time.Millisecond
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state on all servers
	currentTerm int
	votedFor    int
	log         []LogMsg

	// volatile state on all servers
	commitIndex int
	lastApplied int
	status      int

	// volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// chan
	applyMsg             chan ApplyMsg
	appendEntriesChan    chan bool
	requestVoteChan      chan bool
	electionFinishedChan chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	if rf.status == LEADER {
		isleader = true
	} else {
		isleader = false
	}
	return term, isleader
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
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	reply.Term = args.Term
	reply.VoteGranted = false

	// FIXME : refuse vote correctly

	if args.Term < rf.currentTerm {
		return
	}

	myLastLogIndex := 0
	myLastLogTerm := 0
	if len(rf.log) > 0 {
		myLastLogIndex = len(rf.log)
		myLastLogTerm = rf.log[len(rf.log)-1].term
	}

	// at least same up-to-date
	if args.LastLogTerm < myLastLogTerm ||
		(args.LastLogTerm == myLastLogTerm && args.LastLogIndex < myLastLogIndex) {
		return
	}

	// FIXME : may be deleted
	if args.Term == rf.currentTerm && rf.votedFor != -1 {
		return
	}

	rf.mu.Lock()
	reply.VoteGranted = true
	rf.currentTerm = args.Term
	rf.votedFor = args.CandidateId
	rf.mu.Unlock()
	rf.requestVoteChan <- true

	return
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

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return Value is the index that the command will appear at
// if it's ever committed. the second return Value is the current
// Term. the third return Value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	if rf.status != LEADER {
		isLeader = false
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
	rf.status = -1
}

type Entry struct {
	Key   int
	Value int
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Term = rf.currentTerm
	reply.Success = false
	// FIXME : refuse appendEntries correctly
	if args.Term < rf.currentTerm {
		return
	}
	if args.Entries == nil {
		// FIXME : correct currentTerm
		// heartbeat
		rf.mu.Lock()
		rf.currentTerm = args.Term
		rf.status = FOLLOWER
		rf.votedFor = args.LeaderId
		reply.Success = true
		rf.mu.Unlock()
		rf.appendEntriesChan <- true
		DPrintf("[appendEntries] : me=%v, term=%v, votedfor=%v\n", rf.me, rf.currentTerm, rf.votedFor)
	} else {
		// TODO : append entries
		rf.appendEntriesChan <- true
	}
}

// Append Entries
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) setFollower(newTerm int) {
	DPrintf("==================================================\n")
	DPrintf("============== %v becomes FOLLOWER ================\n", rf.me)
	DPrintf("==================================================\n")
	rf.currentTerm = newTerm
	rf.status = FOLLOWER
	rf.votedFor = -1
}

func (rf *Raft) setCandidate() {
	DPrintf("====================================================\n")
	DPrintf("============== %v becomes CANDIDATE =================\n", rf.me)
	DPrintf("====================================================\n")
	rf.currentTerm = rf.currentTerm + 1
	rf.status = CANDIDATE
	rf.votedFor = -1

	go rf.election()
}

func (rf *Raft) setLeader() {
	DPrintf("================================================\n")
	DPrintf("============== %v becomes LEADER ================\n", rf.me)
	DPrintf("================================================\n")

	rf.status = LEADER
	rf.votedFor = rf.me

}

func (rf *Raft) election() {

	DPrintf("[election] : %v participate in the election! term=%v, votefor=%v, status=%v!\n",
		rf.me, rf.currentTerm, rf.votedFor, rf.status)
	var count int32
	count = 1
	var group sync.WaitGroup
	rf.votedFor = rf.me

	for i := 0; i < len(rf.peers); i++ {
		if rf.me == i {
			continue
		}
		group.Add(1)
		args := RequestVoteArgs {
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: 0,
			LastLogTerm:  0,
		}
		// FIXME: args
		if len(rf.log) > 1 {
			args.LastLogIndex = len(rf.log) - 1
			args.LastLogTerm = rf.log[len(rf.log)-1].term
		}
		go func(server int) {
			var reply RequestVoteReply
			// no reply
			ok := rf.sendRequestVote(server, &args, &reply);
			defer group.Done()
			if rf.status != CANDIDATE {
				return
			}
			if !ok || reply.VoteGranted == false {
				return
			} else {
				atomic.AddInt32(&count, 1)
			}

			rf.mu.Lock()
			if rf.status == CANDIDATE && 2 * count > int32(len(rf.peers)) {
				rf.setLeader()
				rf.mu.Unlock()
				rf.electionFinishedChan <- true
			} else {
				rf.mu.Unlock()
			}
		} (i)

	}
	group.Wait()
	DPrintf("[election finish] : %v election finished! get %v votes! status = %v, term = %v\n", rf.me, count, rf.status, rf.currentTerm)
}

func (rf *Raft) heartBeat() {
	var count int32
	count = 1
	var group sync.WaitGroup

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		group.Add(1)
		go func(server int) {
			args := AppendEntriesArgs {
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[server],
				Entries:      nil,
				LeaderCommit: rf.commitIndex,
			}
			var reply AppendEntriesReply
			//DPrintf("[heartbeat] : from %v to %v, beginning!\n", rf.me, server)
			ok := rf.sendAppendEntries(server, &args, &reply)
			defer group.Done()

			rf.mu.Lock()
			if rf.status == FOLLOWER {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()

			if !ok {
				//DPrintf("[heartbeat] : from %v to %v, no response!\n", rf.me, server)
			} else if reply.Success {
				//DPrintf("[heartbeat] : from %v to %v, success = %v\n", rf.me, server, reply.Success)
				atomic.AddInt32(&count, 1)
			} else {
				if reply.Term > rf.currentTerm {
					rf.setFollower(rf.currentTerm)
				}
				//DPrintf("[heartbeat] : from %v to %v, success = %v\n", rf.me, server, reply.Success)
			}
		}(i)
	}
	group.Wait()

}

func (rf *Raft) startService() {

	// producer & consumer
	for {
		followerTime := WaitTimeBase + time.Duration(rand.Intn(500))*time.Millisecond
		candidateTime := WaitTimeBase + time.Duration(rand.Intn(500))*time.Millisecond
		switch rf.status {
		case FOLLOWER:
			select {
			case <-rf.requestVoteChan: // request votes
			case <-rf.appendEntriesChan: // heartbeat or append entries
			case <-time.After(followerTime):
				rf.setCandidate()
			}
		case CANDIDATE:
			select {
			case <-rf.electionFinishedChan: // election finished
			case <-rf.appendEntriesChan: // heartbeat or append entries
			case <-rf.requestVoteChan: // request votes
			case <-time.After(candidateTime):
				rf.setCandidate()
			}
		case LEADER:
			go rf.heartBeat()
			time.Sleep(HeartBeatTime)
		default:
			DPrintf("[EXIT] : %v exit!!!!!!!!!!\n", rf.me)
			goto BREAK
		}
	}
BREAK:
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

	n := len(rf.peers)

	// Your initialization code here (2A, 2B, 2C).

	// FIXME: initialize these variables

	rf.me = me
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogMsg, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, n)
	rf.matchIndex = make([]int, n)
	rf.applyMsg = applyCh
	rf.status = FOLLOWER

	rf.appendEntriesChan = make(chan bool)
	rf.requestVoteChan = make(chan bool)
	rf.electionFinishedChan = make(chan bool)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.startService()

	//time.Sleep(ElectionTimeout * 3)
	return rf
}

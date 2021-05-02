package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(Command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"sort"

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
	Term    int
	Command interface{}
}

const (
	FOLLOWER  = 0
	CANDIDATE = 1
	LEADER    = 2
)

const (
	HEARTBEAT = 0
	SYNC      = 1
)
const (
	ElectionTimeout = 1000 * time.Millisecond
	WaitTimeBase    = 400 * time.Millisecond
	HeartBeatTime   = 150 * time.Millisecond
	RetryTime       = 50 * time.Millisecond
	SyncTime        = time.Second
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
	applyMsg               chan ApplyMsg
	appendEntriesChan      chan bool
	requestVoteChan        chan bool
	electionFinishedChan   chan bool
	synchonizeFinishedChan chan bool

	// optimizer
	electionTimes int
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
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.status = -1
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogMsg
	LeaderCommit int
	EntryType    int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	//matchIndex int
}

//////////////////////////////////////////////////
/////////////////////HELPER///////////////////////
//////////////////////////////////////////////////
func (rf *Raft) getCommitIndex() int {
	var matchIndex []int
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		matchIndex = append(matchIndex, rf.matchIndex[i])
	}
	sort.Ints(matchIndex)
	//if matchIndex[len(rf.peers)/2] > 1 {
	//	//DPrintf("matchIndex = %v\n", matchIndex)
	//}
	return matchIndex[len(rf.peers)/2]
}
func (rf *Raft) getAppendEntriesArgs(heartbeat bool, server int, entryType int) (AppendEntriesArgs, int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var args AppendEntriesArgs
	if heartbeat {
		args = AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.matchIndex[server],
			PrevLogTerm:  rf.log[rf.matchIndex[server]].Term,
			Entries:      nil,
			LeaderCommit: rf.commitIndex,
			EntryType: entryType,
		}
		return args, -1
	} else {
		entries, logSize := rf.getAppendEntries(server)
		args = AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.matchIndex[server],
			PrevLogTerm:  rf.log[rf.matchIndex[server]].Term,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
			EntryType: entryType,
		}
		return args, logSize
	}
}
func (rf *Raft) PrintMsg(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.printLog()
}
func (rf *Raft) printAllMsg(s string) {
	//return
	DPrintf("=================PRINT MSG START(%v)=================\n",s)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			rf.printLog()
			continue
		}
		var args RequestVoteArgs
		var reply RequestVoteReply
		rf.peers[i].Call("Raft.PrintMsg", &args, &reply)
	}
	DPrintf("==================PRINT MSG END(%v)==================\n",s)
}

func (rf *Raft) setFollower(newTerm int) {
	MPrint("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n")
	MPrint("~~~~~~~~~~~~~~ %v becomes FOLLOWER ~~~~~~~~~~~~~~~~\n", rf.me)
	MPrint("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n")
	rf.currentTerm = newTerm
	rf.status = FOLLOWER
	rf.votedFor = -1
	rf.electionTimes = 0
	for i := 0; i < len(rf.peers); i++ {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = 1
	}
}

func (rf *Raft) setCandidate() {

	if rf.status == LEADER {
		return
	}

	DPrintf("====================================================\n")
	DPrintf("========= %v becomes CANDIDATE (term = %v) ===========\n", rf.me, rf.currentTerm+1)
	DPrintf("====================================================\n")

	//fmt.Printf("====================================================\n")
	//fmt.Printf("========= %v becomes CANDIDATE (term = %v) ===========\n", rf.me, rf.currentTerm+1)
	//fmt.Printf("====================================================\n")

	rf.currentTerm = rf.currentTerm + 1
	rf.status = CANDIDATE
	rf.votedFor = -1
	rf.electionTimes++

	for i := 0; i < len(rf.peers); i++ {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = 1
	}

	//rf.printLog()

	//if rf.electionTimes > 10 {
	//	rf.status = -1
	//	return
	//}

	go rf.election()
}

func (rf *Raft) setLeader() {
	MPrint("################################################\n")
	MPrint("############## %v becomes LEADER ################\n", rf.me)
	MPrint("################################################\n")

	rf.electionFinishedChan <- true

	rf.status = LEADER
	rf.votedFor = rf.me
	rf.electionTimes = 0

	// FIXME : correctly initialize nextIndex[] and matchIndex[]
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		rf.nextIndex[i] = 1
		rf.matchIndex[i] = 0
	}

	rf.mu.Unlock()

	rf.startSyncService()

	//go rf.printAllMsg()
	//rf.sync()

}


func (rf *Raft) getAppendEntries(server int) ([]LogMsg, int) {
	var msg []LogMsg

	//rf.mu.Lock()
	if rf.nextIndex[server] < len(rf.log) {
		msg = rf.log[rf.nextIndex[server]:len(rf.log)]
	}
	logSize := len(rf.log)
	//rf.mu.Unlock()

	return msg, logSize
}

func min(x int, y int) int {
	if x > y {
		return y
	} else {
		return x
	}
}
func max(x int, y int) int {
	if x < y {
		return y
	} else {
		return x
	}
}

func (rf *Raft) printLog() {
	MPrint("[me = %v], currentTerm = %v, status = %v, votedFor = %v, lastApplied = %v, commitIndex = %v, log = %v\n",
		rf.me, rf.currentTerm, rf.status, rf.votedFor, rf.lastApplied, rf.commitIndex, rf.log)
	if rf.status == LEADER {
		MPrint("[me = %v], matchIndex = %v\n", rf.me, rf.matchIndex)
	}
}

//////////////////////////////////////////////////
///////////////////OPERATIONS/////////////////////
//////////////////////////////////////////////////


//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	rf.mu.Lock()
	if rf.status == LEADER {
		rf.setFollower(args.Term)
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	//DPrintf("[test zuse] : from %v to %v, start!\n",args.CandidateId,rf.me)
	rf.requestVoteChan <- true

	// FIXME : refuse vote correctly
	if args.Term < rf.currentTerm {
		return
	}

	myLastLogIndex := 0
	myLastLogTerm := 0
	if len(rf.log) > 0 {
		myLastLogIndex = len(rf.log)
		myLastLogTerm = rf.log[len(rf.log)-1].Term
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

	//DPrintf("[test zuse1] : from %v to %v, finish1!\n",args.CandidateId,rf.me)

	rf.mu.Lock()
	DPrintf("[LOCK] : %v acquire LOCK\n",rf.me)

	reply.VoteGranted = true
	rf.currentTerm = args.Term
	rf.votedFor = args.CandidateId
	rf.mu.Unlock()
	DPrintf("[UNLOCK] : %v release LOCK\n",rf.me)
	//DPrintf("[test zuse2] : from %v to %v, finish2!\n",args.CandidateId,rf.me)

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
	//start := time.Now()
	//DPrintf("[request vote time] : from %v to %v, trying...\n", rf.me, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	//DPrintf("[request vote time] : %v, from %v to %v, ok = %v, success = %v\n", time.Since(start), rf.me, server, ok, reply.VoteGranted)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Term = rf.currentTerm
	reply.Success = false

	// FIXME : refuse appendEntries correctly
	if args.Term < rf.currentTerm {
		return
	}
	if args.EntryType == HEARTBEAT {

		rf.appendEntriesChan <- true
		rf.mu.Lock()
		DPrintf("[LOCK] : %v acquire LOCK\n",rf.me)

		reply.Success = true
		rf.currentTerm = args.Term
		rf.status = FOLLOWER
		rf.votedFor = args.LeaderId

		rf.mu.Unlock()
		DPrintf("[UNLOCK] : %v release LOCK\n",rf.me)

	} else {

		if !(len(rf.log) > args.PrevLogIndex &&
			rf.log[args.PrevLogIndex].Term == args.PrevLogTerm) {
			return
		}


		rf.appendEntriesChan <- true
		reply.Success = true


		rf.mu.Lock()
		DPrintf("[LOCK] : %v acquire LOCK\n",rf.me)


		// delete wrong log
		if args.EntryType != 2 && len(args.Entries) + args.PrevLogIndex + 1 < len(rf.log) {
			rf.log = rf.log[0:len(args.Entries)+args.PrevLogIndex+1]
		}
		for i := 0; i < len(args.Entries); i++ {
			// dont have the log
			if i+args.PrevLogIndex+1 >= len(rf.log) {
				rf.log = append(rf.log, args.Entries[i])
				continue
			}
			// wrong log and replace it
			if args.Entries[i] != rf.log[i+args.PrevLogIndex+1] {
				rf.log[i+args.PrevLogIndex+1] = args.Entries[i]
			}
		}
		prev := rf.lastApplied
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		curr := rf.commitIndex

		rf.mu.Unlock()
		DPrintf("[UNLOCK] : %v release LOCK\n",rf.me)

		if prev != curr {
			DPrintf("[commitIndex update by append entries] : me = %v, leader = %v, last_applied = %v, leader_commit = %v, prev = %v, curr = %v, log = %v, entries[type=%v] = %v\n",
				rf.me, args.LeaderId, rf.lastApplied, args.LeaderCommit, prev, curr, rf.log, args.EntryType,args.Entries)
				rf.sendServerLogMsg(prev, curr)
		}
	}
}

// Append Entries
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	//start := time.Now()
	//DPrintf("[send append entries time] : from %v to %v, args = %v. trying.........\n", rf.me, server, args)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	//DPrintf("[send append entries time] :  %v, from %v to %v, args = %v, ok = %v, success = %v\n", time.Since(start), rf.me, server, args, ok, reply.Success)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next Command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// Command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return Value is the index that the Command will appear at
// if it's ever committed. the second return Value is the current
// Term. the third return Value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	//fmt.Printf("start %v, command = %v, status = %v\n",rf.me,command,rf.status)

	// new leader
	time.Sleep(HeartBeatTime * 2)

	// Your code here (2B).
	if rf.status != LEADER {
		isLeader = false
		return index, term, isLeader
	}

	rf.mu.Lock()

	// append log locally
	rf.log = append(rf.log, LogMsg{
		Term:    rf.currentTerm,
		Command: command,
	})

	//index = rf.getCommitIndex()
	index = len(rf.log) - 1

	rf.mu.Unlock()

	time.Sleep(time.Second)

	rf.mu.Lock()

	if len(rf.log) - 1 < index {
		index = len(rf.log) - 1
	}
	if rf.status != LEADER {
		rf.mu.Unlock()
		//fmt.Printf("me = %v, index = %v, term = %v, isLeader = %v\n", rf.me, index, term, isLeader)
		return index, rf.currentTerm, false
	}

	term = rf.currentTerm
	rf.mu.Unlock()

	DPrintf("[start] : me = %v, commitIndex = %v, log = %v\n",rf.me,rf.commitIndex,rf.log)
	//rf.printAllMsg()

	//fmt.Printf("me = %v, index = %v, term = %v, isLeader = %v\n", rf.me, index, term, isLeader)

	return index, term, isLeader
}


func (rf *Raft) syncLog() {
	var group sync.WaitGroup
	var count int32
	set := make(map[int]bool)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		set[i] = false
	}

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me || rf.matchIndex[i]+1 == len(rf.log) {
			continue
		}
		group.Add(1)
		go func(server int) {
			defer group.Done()
		//RETRY:
			args, logSize := rf.getAppendEntriesArgs(false, server, 1)
			var reply AppendEntriesReply

			DPrintf("[try update sync log] : me = %v, server = %v, log = %v, logSize = %v\n",rf.me,server,args.Entries,logSize)
			ok := rf.sendAppendEntries(server, &args, &reply)
			DPrintf("[try update sync log] : me = %v, server = %v, log = %v, reply = %v, ok = %v, success = %v\n",
				rf.me,server,args.Entries,reply,ok,reply.Success)


			rf.mu.Lock()
			DPrintf("[LOCK] : %v acquire LOCK\n",rf.me)


			if rf.status != LEADER {
				rf.currentTerm = max (rf.currentTerm, reply.Term)
				rf.mu.Unlock()
				DPrintf("[UNLOCK] : %v release LOCK\n",rf.me)
				return
			}

			rf.mu.Unlock()
			DPrintf("[UNLOCK] : %v release LOCK\n",rf.me)
			//DPrintf("out!!!!!!!!!!!!\n")

			// FIXME : may need some LOCKS
			if !ok {
				return
			} else if reply.Success {
				//DPrintf("[matchIndex update] :  %v's matchIndex = %v --> %v\n",server,rf.matchIndex[server], logSize-1)
				rf.nextIndex[server] = logSize
				rf.matchIndex[server] = logSize - 1
				atomic.AddInt32(&count, 1)
				rf.mu.Lock()
				set[server] = true
				rf.mu.Unlock()
				if 2 * count > int32(len(rf.peers)) && rf.status == LEADER {
					commitIndex := rf.getCommitIndex()
					if commitIndex > rf.commitIndex {
						rf.commitIndex = commitIndex
						go rf.sendServerLogMsg(rf.lastApplied, rf.commitIndex)
					}
					rf.mu.Lock()
					updated := make([]int, 0)
					for i := range set{
						if set[i] == false {
							continue
						}
						updated = append(updated, i)
						go rf.updateAppliedLog(i)
					}
					for i := 0; i < len(updated); i++ {
						delete(set, updated[i])
					}
					rf.mu.Unlock()
				}
			} else {
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.setFollower(reply.Term)
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
				rf.nextIndex[server]--
				rf.matchIndex[server]--
				//time.Sleep(RetryTime)
				//DPrintf("retry sync log..., args = %v, reply = %v\n", args, reply)
				//goto RETRY
			}
		}(i)
	}
	group.Wait()
}

func (rf *Raft) updateAppliedLog(server int) bool {
	if server == rf.me || rf.matchIndex[server] != rf.commitIndex {
		return false
	}
	success := false
	var group sync.WaitGroup
	group.Add(1)
	go func(server int)  {
		defer group.Done()
		args, _ := rf.getAppendEntriesArgs(true, server, 2)
		var reply AppendEntriesReply
		DPrintf("[try update applied log] : me = %v, server = %v, commitIndex = %v\n",rf.me,server,args.LeaderCommit)
		ok := rf.sendAppendEntries(server, &args, &reply)
		DPrintf("[try update applied log] : me = %v, server = %v, commitIndex = %v, ok = %v, success = %v\n",
			rf.me,server,args.LeaderCommit,ok,reply.Success)
		rf.mu.Lock()
		if rf.status != LEADER {
			rf.currentTerm = max (rf.currentTerm, reply.Term)
			rf.mu.Unlock()
			return
		}
		if reply.Success {
			success = true
		} else {
			if reply.Term > args.Term && rf.status == LEADER {
				rf.setFollower(reply.Term)
			}
		}
		rf.mu.Unlock()
	}(server)

	group.Wait()
	return success
}

func (rf *Raft) sendServerLogMsg(prev int, curr int) {
	if max(1, prev+1) <= curr {
		DPrintf("[sendServerLogMsg] : %v send log[%v-%v] to server! log_length = %v\n", rf.me, max(1, prev+1), curr, len(rf.log))
		for i := max(1, prev+1); i <= curr; i++ {
			rf.applyMsg <- ApplyMsg{
				CommandValid: true,
				Command:      rf.log[i].Command,
				CommandIndex: i,
			}
		}
		rf.lastApplied = curr
	}
}

func (rf *Raft) sync() {

	//rf.printAllMsg("sync before")
	qwq := rand.Int() % 20
	DPrintf("[id = %v] : %v sync log....!\n",qwq,rf.me)
	rf.syncLog()
	DPrintf("[id = %v] : %v syncLog Done!!!!\n",qwq,rf.me)
	//rf.printAllMsg("sync after")

	rf.mu.Lock()
	DPrintf("[LOCK] : %v acquire LOCK\n",rf.me)

	if rf.status != LEADER {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()
	DPrintf("[sync] : me = %v, log = %v\n",rf.me,rf.log)
	DPrintf("sync Done!\n")
}

func (rf *Raft) election() {

	//DPrintf("[election] : %v participate in the election! Term=%v, votefor=%v, status=%v!\n",
	//	rf.me, rf.currentTerm, rf.votedFor, rf.status)
	var count int32
	count = 1
	var group sync.WaitGroup
	rf.votedFor = rf.me


	var voteGrantedSet, successSet []int

	maxTerm := 0

	for i := 0; i < len(rf.peers); i++ {
		if rf.me == i {
			continue
		}
		group.Add(1)
		args := RequestVoteArgs {
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: len(rf.log),
			LastLogTerm:  rf.log[len(rf.log)-1].Term,
		}
		go func(server int) {
			var reply RequestVoteReply
			// no reply
			ok := rf.sendRequestVote(server, &args, &reply)
			defer group.Done()
			rf.mu.Lock()
			DPrintf("[LOCK] : %v acquire LOCK\n",rf.me)

			if ok {
				successSet = append(successSet, server)
			}
			if reply.VoteGranted {
				voteGrantedSet = append(voteGrantedSet, server)
			}

			maxTerm = max (maxTerm, reply.Term)

			if rf.status != CANDIDATE {
				//rf.currentTerm = max (rf.currentTerm, reply.Term)
				rf.mu.Unlock()
				return
			}

			if !ok || reply.VoteGranted == false {
				//rf.currentTerm = max (rf.currentTerm, reply.Term)
				rf.mu.Unlock()
				return
			} else {
				atomic.AddInt32(&count, 1)
			}

			if rf.status == CANDIDATE && 2*count > int32(len(rf.peers)) {

				DPrintf("[BECOME LEADER] : %v got %v votes including %v!\n",rf.me,count,voteGrantedSet)
				rf.setLeader()
				DPrintf("[UNLOCK] : %v release LOCK\n",rf.me)
			} else {
				rf.mu.Unlock()
				DPrintf("[UNLOCK] : %v release LOCK\n",rf.me)
			}
		}(i)

	}
	group.Wait()

	rf.mu.Lock()
	DPrintf("[election finish] : me = %v, status = %v, vote = %v, success = %v\n",rf.me,rf.status,voteGrantedSet,successSet)
	DPrintf("%v's term is toooooooooo old! currentTerm = %v, maxTerm = %v\n",rf.me,rf.currentTerm,maxTerm)

	// FIXME: current term
	if rf.status != LEADER {
		rf.currentTerm = max(rf.currentTerm, maxTerm + 1)
	}
	rf.mu.Unlock()

	//DPrintf("[election finish] : %v election finished! get %v votes! status = %v, Term = %v, votedSet = %v\n",
	//	rf.me, count, rf.status, rf.currentTerm, getVotedSet)
}

func (rf *Raft) heartBeat() {
	var group sync.WaitGroup
	maxTerm := 0

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		group.Add(1)
		go func(server int) {
			args, _ := rf.getAppendEntriesArgs(true, server, 0)
			var reply AppendEntriesReply

			rf.sendAppendEntries(server, &args, &reply)

			//start := time.Now()
			//DPrintf("[heartbeat] : from %v to %v, trying...\n", rf.me,server)
			//ok := rf.sendAppendEntries(server, &args, &reply)
			//DPrintf("[heartbeat] : from %v to %v, time = %v, ok = %v, success = %v\n",rf.me,server,time.Since(start), ok, reply.Success)
			defer group.Done()

			rf.mu.Lock()
			maxTerm = max(maxTerm, reply.Term)

			DPrintf("[LOCK] : %v acquire LOCK\n",rf.me)
			if rf.status != LEADER {
				//rf.currentTerm = max (rf.currentTerm, reply.Term)
				rf.mu.Unlock()
				DPrintf("[UNLOCK] : %v release LOCK\n",rf.me)
				return
			}
			// FIXME : maybe a bug
			if reply.Term > rf.currentTerm {
				rf.setFollower(reply.Term)
			}
			rf.mu.Unlock()
			DPrintf("[UNLOCK] : %v release LOCK\n",rf.me)
		}(i)
	}
	group.Wait()
	rf.mu.Lock()
	if rf.status != LEADER {
		rf.currentTerm = max (rf.currentTerm, maxTerm)
	}
	rf.mu.Unlock()
}

func (rf *Raft) startPrintLogService() {
	for {
		if rf.status == -1 {
			break
		}
		rf.printLog()
		time.Sleep(time.Duration(500) * time.Millisecond)
	}
}

func (rf *Raft) startSyncService() {
	for {
		if rf.status != LEADER {
			break
		}
		//rf.printAllMsg("sync service")
		needSync := false
		for i := 0; i < len(rf.peers); i++ {
			if rf.matchIndex[i] != len(rf.log) - 1 {
				needSync = true
			}
		}
		if needSync {
			go rf.sync()
		}
		time.Sleep(SyncTime)
	}
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
				DPrintf("receive election finish Chan!\n")
			case <-rf.appendEntriesChan: // heartbeat or append entries
			case <-rf.requestVoteChan: // request votes
			case <-time.After(candidateTime):
				rf.setCandidate()
			}
		case LEADER:
			go rf.heartBeat()
			time.Sleep(HeartBeatTime)
		//default:
		//	DPrintf("[EXIT] : %v exit!!!!!!!!!!\n", rf.me)
		//	goto BREAK
		}
	}
//BREAK:
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
	rf.log = append(rf.log, LogMsg{
		Term:    0,
		Command: nil,
	})
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, n)
	rf.matchIndex = make([]int, n)
	rf.applyMsg = applyCh
	rf.status = FOLLOWER

	rf.appendEntriesChan = make(chan bool)
	rf.requestVoteChan = make(chan bool)
	rf.electionFinishedChan = make(chan bool)
	rf.synchonizeFinishedChan = make(chan bool)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.startService()

	//go rf.startPrintLogService()

	//time.Sleep(ElectionTimeout * 3)
	return rf
}

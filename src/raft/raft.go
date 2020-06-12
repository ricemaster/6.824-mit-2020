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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

var Leader = 0
var Follower = 1
var Candidate = 2

var wg sync.WaitGroup

//
// as each Raft peer becomes aware that successive log entries are
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
	state        int
	lastLogIndex int

	// in Figure 2
	role        int
	currentTerm int
	voteFor     int
	log         map[int]LogEntry

	commitIndex int
	lastApplied int

	nextIndex  map[int]int
	matchIndex map[int]int

	// timeout
	electionTimeout  int
	receiveHeartBeat bool

	// vote
	currentVote map[int]int
}

//LogEntry - Log
type LogEntry struct {
	Index   int
	Term    int
	Command string
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = (rf.state == Leader)
	rf.mu.Unlock()

	// DPrintf("GetStatus: id=%d, term=%d, isleader=%v\n", rf.me, term, isleader)

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
	CandidateID  int
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.receiveHeartBeat = true

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf("Rejected Vote Request: Stale Term - Candidate(id=%d, term=%d), This(id=%d, term=%d)\n", args.CandidateID, args.Term, rf.me, rf.currentTerm)
		return
	}

	if _, exist := rf.currentVote[args.Term]; !exist {
		delete(rf.currentVote, rf.currentTerm)
		rf.voteFor = args.CandidateID
	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf("Rejected Vote Request: Have Voted in current term - Candidate(id=%d, term=%d), This(id=%d, term=%d, currentVote=%v)\n", args.CandidateID, args.Term, rf.me, rf.currentTerm, rf.currentVote)
		return

	}

	if (rf.voteFor == args.CandidateID) &&
		rf.log[args.LastLogIndex].Term == args.LastLogTerm {
		// rf.voteFor = args.CandidateID
		rf.currentTerm = args.Term
		rf.currentVote[rf.currentTerm] = rf.voteFor
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		// DPrintf("Agreed Vote Request: Candidate(id=%d, term=%d), This(id=%d, term=%d)\n", args.CandidateID, args.Term, rf.me, rf.currentTerm)
		return
	}
	DPrintf("Failed to vote: Candidate(id=%d, term=%d), This(id=%d, term=%d)\n", args.CandidateID, args.Term, rf.me, rf.currentTerm)
	// DPrintf("rf.voteFor=%d, args.CandidateID=%d, rf.log[args.LastLogIndex].Term=%d, args.LastLogTerm=%d\n", rf.voteFor, args.CandidateID, rf.log[args.LastLogIndex].Term, args.LastLogTerm)
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

// AppendEntriesArgs - according to Figure 2
type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Enties       []LogEntry
	LeaderCommit int
}

// AppendEntriesReply - according to Figure 2
type AppendEntriesReply struct {
	// Your data here (2A).
	Term    int
	Success bool
}

// AppendEntries - handle AppendEntries RPC
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	rf.receiveHeartBeat = true
	rf.mu.Unlock()

	// DPrintf("HearBeat received: Leader=%d, Follower=%d\n", args.LeaderID, rf.me)

	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	log := rf.log[args.PrevLogIndex]
	if log.Term != args.PrevLogTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	for index, logEntry := range args.Enties {
		entry, exist := rf.log[index]
		if !exist || entry.Term != logEntry.Term {
			rf.log[index] = logEntry
		}
		rf.lastLogIndex = index
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.lastLogIndex)
	}

	rf.mu.Lock()
	rf.state = Follower

	rf.currentTerm = args.Term
	rf.mu.Unlock()

	reply.Success = true
}

func min(x, y int) int {
	if x > y {
		return y
	}
	return x
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		DPrintf("Failed to Sent HeartBeat: from %d to %d\n", rf.me, server)
	}
	rf.mu.Lock()
	if reply.Term > rf.currentTerm {
		rf.state = Follower
		DPrintf("Become Follower: id=%d, term=%d, server=%d, term=%d\n", rf.me, rf.currentTerm, server, reply.Term)
	}
	rf.mu.Unlock()
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

// generate a random timeout between 300~400ms
func generateTimeout() int {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return 300 + r.Intn(100)
}

func (rf *Raft) handleElectionTimeout() {
	for {
		rf.mu.Lock()
		currentState := rf.state
		// DPrintf("--> Working: id=%d, state=%d\n", rf.me, rf.state)
		rf.mu.Unlock()
		switch currentState {
		case Follower:
			rf.mu.Lock()
			rf.receiveHeartBeat = false
			rf.mu.Unlock()

			for i := 0; i < rf.electionTimeout; i++ {
				time.Sleep(time.Millisecond)
				rf.mu.Lock()
				if rf.receiveHeartBeat {
					DPrintf("Reset ElectionTimer: id=%d, state=%d\n", rf.me, rf.state)
					rf.mu.Unlock()
					break
				}
				rf.mu.Unlock()
			}
			rf.mu.Lock()
			if !rf.receiveHeartBeat {
				DPrintf("Become Candidate: id=%d\n", rf.me)
				rf.state = Candidate
			}
			rf.mu.Unlock()
		case Candidate:
			rf.mu.Lock()
			rf.receiveHeartBeat = false
			rf.mu.Unlock()
			DPrintf("Starting Election: id=%d\n", rf.me)
			rf.startElection()

			for i := 0; i < rf.electionTimeout; i++ {
				time.Sleep(time.Millisecond)
				rf.mu.Lock()
				if rf.receiveHeartBeat {
					rf.mu.Unlock()
					break
				}
				rf.mu.Unlock()
			}

		case Leader:
			rf.sendHeartBeat()
			time.Sleep(time.Millisecond * time.Duration(rf.electionTimeout))
		default:
			DPrintf("Invalid State: id=%d, state=%d\n", rf.me, currentState)
		}
	}

}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.currentTerm++

	rf.voteFor = rf.me

	args := RequestVoteArgs{}
	args.CandidateID = rf.me
	args.Term = rf.currentTerm
	args.LastLogIndex = rf.lastLogIndex
	args.LastLogTerm = rf.log[rf.lastLogIndex].Term

	// vote self
	voteGranted := 1
	for server := range rf.peers {
		if server != rf.me {
			// wg.Add(1)
			go func(server int, rf *Raft) {
				// DPrintf("Starting request vote: from %d to %d\n", rf.me, server)

				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(server, &args, &reply)

				DPrintf("Received vote response: server=%d, voted=%v", server, reply)

				if ok {
					rf.mu.Lock()
					currentStat := rf.state
					rf.mu.Unlock()
					if currentStat == Leader {
						return
					}

					if reply.VoteGranted {
						voteGranted++
						// DPrintf("total voted: id=%d, votes=%d\n", rf.me, voteGranted)
					}
					// DPrintf("votes=%d, majority=%d", voteGranted, len(rf.peers)/2)
					if voteGranted > len(rf.peers)/2 {
						rf.mu.Lock()
						rf.state = Leader
						DPrintf("Successful Become Leader: id=%d\n", rf.me)
						rf.mu.Unlock()
						rf.sendHeartBeat()
						return
					}
				} else {
					DPrintf("Failed call[Raft.RequestVote]: id=%d, from=%d\n", server, rf.me)
				}
				// wg.Done()
			}(server, rf)
		}
	}
	// wg.Wait()
}

func (rf *Raft) sendHeartBeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	args := AppendEntriesArgs{}
	args.Term = rf.currentTerm
	args.LeaderID = rf.me
	args.PrevLogIndex = rf.lastLogIndex
	args.PrevLogTerm = rf.log[rf.lastLogIndex].Term
	args.Enties = nil
	args.LeaderCommit = rf.commitIndex

	for server := range rf.peers {
		if server != rf.me {
			reply := AppendEntriesReply{}
			DPrintf("Send HeartBeat: from %d to %d\n", rf.me, server)
			go rf.sendAppendEntries(server, &args, &reply)
		}
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
	rf.state = Follower
	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.voteFor = -1 // -1 means null
	rf.nextIndex = make(map[int]int)
	rf.matchIndex = make(map[int]int)
	rf.currentVote = make(map[int]int)

	rf.lastLogIndex = 1
	rf.log = make(map[int]LogEntry)

	rf.electionTimeout = generateTimeout()
	// switch rf.me {
	// case 0:
	// 	rf.electionTimeout = 300
	// default:
	// 	rf.electionTimeout = 3000
	// }
	rf.receiveHeartBeat = false

	DPrintf("NEW Raft: id=%d, state=%d, peers=%v, ElectionTimeout=%d\n", rf.me, rf.state, rf.peers, rf.electionTimeout)
	go rf.handleElectionTimeout()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

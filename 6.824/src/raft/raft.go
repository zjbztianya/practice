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

import "sync"
import (
	"labrpc"
	"sort"
	"time"
)

// import "bytes"
// import "encoding/gob"

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

//
// server states
//
type ServerStates uint8

const (
	Leader    ServerStates = 1
	Follower               = 2
	Candidate              = 3
)

const (
	HeartbeatTimeout = 50 * time.Millisecond
	ElectionTimeout  = 150 * time.Millisecond
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

	// Persistent state on all servers,Updated on stable storage before responding to RPCs
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	state ServerStates

	applyCh       chan ApplyMsg
	voteCh        chan RequestVoteReply
	roleChangeCh  chan bool
	electionTimer *time.Timer
}

//
// log entry structure
//
type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isLeader bool
	// Your code here (2A).
	term = rf.GetTerm()
	if rf.State() == Leader {
		isLeader = true
	}
	return term, isLeader
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
// Invoked by leader to replicate log entries, also used as heartbeat.
//
type RequestAppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	Entries      []LogEntry
}

//
// example AppendEntriesRequest RPC reply structure.
//
type RequestAppendEntriesReply struct {
	Term    int
	Success bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	currentTerm := rf.GetTerm()
	state := rf.State()
	reply.Term = currentTerm
	reply.VoteGranted = false
	if args.Term < currentTerm {
		return
	}

	if args.Term > currentTerm {
		rf.updateTerm(args.Term)
	}

	if rf.getVoteFor() != -1 {
		return
	}

	lastLogTerm, lastLogIndex := rf.lastLogInfo()
	if lastLogTerm < args.LastLogTerm || (lastLogTerm == args.LastLogTerm && lastLogIndex <= args.LastLogIndex) {
		reply.VoteGranted = true
		reply.Term = args.Term
		rf.setVoteFor(args.CandidateId)
		if state == Follower {
			rf.resetElectionTimer()
		}
		return
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		return ok
	}
	if rf.State() != Candidate {
		return false
	}
	return ok
}

func (rf *Raft) processRequestVoteReply(reply *RequestVoteReply) bool {
	currentTerm := rf.GetTerm()
	if reply.VoteGranted && currentTerm == reply.Term {
		return true
	}
	// Discover higher term: step down
	if reply.Term > currentTerm {
		rf.updateTerm(reply.Term)
	}

	return false
}

//
//
//
func (rf *Raft) RequestAppendEntries(args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
	currentTerm := rf.GetTerm()
	reply.Term = currentTerm
	reply.Success = false

	if args.Term < currentTerm {
		return
	}

	if args.Term == currentTerm {
		if rf.State() == Candidate {
			rf.setState(Follower)
		}
	}

	if args.Term > currentTerm {
		rf.updateTerm(args.Term)
	}

	rf.resetElectionTimer()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	if len(rf.log) <= args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		return
	}

	reply.Success = true
	if rf.commitIndex <= args.PrevLogIndex {
		rf.log = rf.log[:args.PrevLogIndex+1]
	}

	if len(args.Entries) > 0 {
		rf.log = append(rf.log, args.Entries...)
	}

	if args.LeaderCommit > rf.commitIndex {
		commitIndex := Min(args.LeaderCommit, rf.log[len(rf.log)-1].Index)
		for i := rf.commitIndex + 1; i <= commitIndex; i++ {
			rf.applyCh <- ApplyMsg{Index: i, Command: rf.log[i].Command}
		}
		rf.commitIndex = commitIndex
	}
}

func (rf *Raft) sendRequestAppendEntries(server int, args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.RequestAppendEntries", args, reply)
	if !ok {
		return ok
	}
	if rf.State() != Leader {
		return false
	}
	currentTerm := rf.GetTerm()
	if reply.Term < currentTerm {
		return ok
	}

	if reply.Term > currentTerm {
		rf.updateTerm(reply.Term)
		return ok
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !reply.Success {
		rf.nextIndex[server] = rf.nextIndex[server] - 1
		return ok
	}

	rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
	rf.nextIndex[server] = rf.matchIndex[server] + 1
	if rf.commitIndex < rf.matchIndex[server] {
		a := make([]int, len(rf.peers))
		copy(a, rf.matchIndex)
		sort.Ints(a)
		index := a[(len(rf.peers)+1)/2]
		if index > rf.commitIndex && rf.log[index].Term == currentTerm {
			for i := rf.commitIndex + 1; i <= index; i++ {
				rf.applyCh <- ApplyMsg{Index: i, Command: rf.log[i].Command}
			}
			rf.commitIndex = index
		}
	}
	return ok
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
	index := -1
	term := -1
	isLeader := false

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2B).
	if rf.state == Leader {
		isLeader = true
		index = len(rf.log)
		term = rf.currentTerm
		rf.log = append(rf.log, LogEntry{term, index, command})
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
}

func (rf *Raft) State() ServerStates {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state
}

func (rf *Raft) setState(s ServerStates) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	preState := rf.state
	rf.state = s
	if preState != s {
		go func() { rf.roleChangeCh <- true }()
	}
}

func (rf *Raft) lastLogInfo() (int, int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	lastLog := rf.log[len(rf.log)-1]
	return lastLog.Term, lastLog.Index
}

func (rf *Raft) majority(votes int) bool {
	return votes > (len(rf.peers) / 2)
}

func (rf *Raft) updateTerm(term int) {
	rf.mu.Lock()
	rf.currentTerm = term
	rf.votedFor = -1
	rf.mu.Unlock()
	rf.setState(Follower)
}

func (rf *Raft) GetTerm() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm
}

func (rf *Raft) incTerm() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm++
}

func (rf *Raft) getVoteFor() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.votedFor
}

func (rf *Raft) setVoteFor(v int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.votedFor = v

}

func (rf *Raft) startElectionTimer() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.electionTimer = time.NewTimer(AfterBetween(ElectionTimeout, 2*ElectionTimeout))
}

func (rf *Raft) stopElectionTimer() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.electionTimer.Stop()
}

func (rf *Raft) resetElectionTimer() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.electionTimer.Reset(AfterBetween(ElectionTimeout, 2*ElectionTimeout))
}

func (rf *Raft) getElectionTimeOutChan() <-chan time.Time {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.electionTimer.C
}

func (rf *Raft) starHeartbeat() {
	for i := range rf.peers {
		if i != rf.me {
			go func(server int) {
				ticker := time.NewTicker(HeartbeatTimeout)
				for rf.State() == Leader {
					rf.mu.Lock()
					nextIndex := rf.nextIndex[server]
					args := RequestAppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						LeaderCommit: rf.commitIndex,
						PrevLogIndex: nextIndex - 1,
						PrevLogTerm:  rf.log[nextIndex-1].Term,
					}
					args.Entries = make([]LogEntry, len(rf.log[nextIndex:]))
					copy(args.Entries, rf.log[nextIndex:])
					rf.mu.Unlock()
					go func(args RequestAppendEntriesArgs) {
						var reply RequestAppendEntriesReply
						rf.sendRequestAppendEntries(server, &args, &reply)
					}(args)
					<-ticker.C
				}
				ticker.Stop()
			}(i)
		}
	}
}

func (rf *Raft) leaderLoop() {
	rf.mu.Lock()
	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}
	rf.mu.Unlock()
	rf.starHeartbeat()
	select {
	case <-rf.roleChangeCh:
		return
	}
}

func (rf *Raft) followerLoop() {
	rf.startElectionTimer()
	defer rf.stopElectionTimer()
	for {
		select {
		case <-rf.electionTimer.C:
			rf.setState(Candidate)
		case <-rf.roleChangeCh:
			return
		}
	}
}

func (rf *Raft) candidateLoop() {
	doVote := true
	votesGranted := 0
	lastLogTerm, lastLogIndex := rf.lastLogInfo()
	rf.voteCh = make(chan RequestVoteReply, len(rf.peers))
	rf.startElectionTimer()
	defer rf.stopElectionTimer()
	for {
		if doVote {
			rf.incTerm()
			rf.setVoteFor(rf.me)
			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				go func(server int) {
					args := RequestVoteArgs{
						Term:         rf.GetTerm(),
						CandidateId:  rf.me,
						LastLogTerm:  lastLogTerm,
						LastLogIndex: lastLogIndex,
					}
					var reply RequestVoteReply
					if rf.sendRequestVote(server, &args, &reply) {
						rf.voteCh <- reply
					}
				}(i)
			}
			votesGranted = 1
			doVote = false
		}

		select {
		case reply := <-rf.voteCh:
			if rf.processRequestVoteReply(&reply) {
				votesGranted++
				rf.resetElectionTimer()
				if rf.majority(votesGranted) {
					rf.setState(Leader)
				}
			}
		case <-rf.getElectionTimeOutChan():
			doVote = true
			rf.resetElectionTimer()
		case <-rf.roleChangeCh:
			return
		}
	}
}

func (rf *Raft) mainLoop() {
	for {
		switch rf.State() {
		case Leader:
			rf.leaderLoop()
		case Follower:
			rf.followerLoop()
		case Candidate:
			rf.candidateLoop()
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
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []LogEntry{}
	rf.log = append(rf.log, LogEntry{0, 0, nil})
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.applyCh = applyCh
	rf.roleChangeCh = make(chan bool)
	rf.state = Follower
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go func() {
		rf.mainLoop()
	}()
	return rf
}

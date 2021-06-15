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
	"labrpc"
	"math"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

// ApplyMsg explanation
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool
	Snapshot    []byte
}

//Entry is one unit of log
type Entry struct {
	Term    int
	Command interface{}
}

// Raft explanation
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	timeoutElection    int
	heartbeatTimer     int
	heartbeatFrequency int
	currTerm           int
	leaderID           int
	votedFor           int
	isCandidate        bool
	appendEntriesChan  chan AppendEntriesArgs
	requestVoteChan    chan RequestVoteReply
	quitChan           chan int
	quitLeaderChan     chan int
	votes              int
	won                bool

	// ! Assignment 3 //
	// Log
	emptyValue       Entry
	myLog            []Entry
	commitIndex      int
	lastAppliedIndex int

	//leader states
	peersNextIndex  []int
	peersMatchIndex []int

	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// GetState should
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	rf.mu.Lock()
	term = rf.currTerm
	isleader = rf.leaderID == rf.me
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	rf.mu.Lock()
	e.Encode(rf.currTerm)
	e.Encode(rf.myLog)
	e.Encode(rf.commitIndex)
	e.Encode(rf.lastAppliedIndex)
	e.Encode(rf.peersNextIndex)
	rf.mu.Unlock()
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	rf.mu.Lock()
	d.Decode(&rf.currTerm)
	d.Decode(&rf.myLog)
	d.Decode(&rf.commitIndex)
	d.Decode(&rf.lastAppliedIndex)
	d.Decode(&rf.peersNextIndex)
	rf.leaderID = -1
	rf.votedFor = -1
	if len(rf.myLog) == 0 {
		rf.emptyValue = Entry{-99, nil}
		rf.myLog = append(rf.myLog, rf.emptyValue)
	}
	rf.mu.Unlock()

}

// RequestVoteArgs should
// example RequestVote RPC arguments structure.
//! modified
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	ID           int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply exmaple
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

// RequestVote example
// example RequestVote RPC handler.
// This is called whenever a requestVote call is made to a server (implement what I will do when its called on me)
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.

	// Not voting by default
	rf.mu.Lock()

	reply.VoteGranted = false
	reply.Term = args.Term

	// Setting the reply term to be currentTerm if the candidate is outdated
	if args.Term < rf.currTerm {
		reply.Term = rf.currTerm
		rf.mu.Unlock()
		return
	}

	// give vote if the server hasnt voted for this term and the requesters term is greater //! Check up-to-date : need to modify
	if (rf.votedFor == -1 || rf.votedFor == args.ID) && (args.Term >= rf.currTerm) && (rf.me != rf.leaderID) {

		prevLogI := len(rf.myLog) - 1
		prevLogT := -1
		if prevLogI >= 0 {
			prevLogT = rf.myLog[prevLogI].Term
		}

		//Check last entries term if they tie then check the index greater wins. If my log is more updated decline
		if prevLogT > args.LastLogTerm || ((prevLogT == args.LastLogTerm) && (prevLogI > args.LastLogIndex)) {
			reply.VoteGranted = false
		} else {
			reply.VoteGranted = true
			reply.Term = args.Term
			// keep record for who the server voted for
			rf.votedFor = args.ID
		}
	}
	rf.mu.Unlock()

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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	// if rejected check term and update the term
	if ok {
		rf.mu.Lock()
		if !reply.VoteGranted && reply.Term > rf.currTerm {
			rf.currTerm = reply.Term
		}

		if reply.VoteGranted {
			rf.votes++
			//fmt.Printf("%d voted for %d - now has %d votes\n", server, rf.me, rf.votes)
		}
		rf.mu.Unlock()

		//fmt.Printf("Got vote reply for %d\n", server)
		//check if I have majority of votes
		//rf.mu.Lock()
		rf.mu.Lock()
		proportion := float32(float32(rf.votes) / float32(len(rf.peers)))
		rf.mu.Unlock()
		//fmt.Printf("%d has proportion %f\n", rf.me, proportion)
		//rf.mu.Unlock()

		rf.mu.Lock()
		if proportion > 0.5 && rf.won == false {
			// I am the one dont weigh a ton Dont need a gun to get respect up on the street

			rf.won = true
			// set state as leader and start sending heart beats
			rf.votedFor = rf.me
			rf.leaderID = rf.me
			rf.heartbeatTimer = 0
			rf.isCandidate = false
			//fmt.Printf("starting leader thread\n")
			go rf.Leader()

		} else if rf.won == false {

			rf.votedFor = -1
			rf.leaderID = -1
			rf.isCandidate = false

		}
		rf.mu.Unlock()
	}
	return ok
}

// AppendEntriesArgs structure for arguments
type AppendEntriesArgs struct { //! Modified
	Term              int
	ID                int
	PrevLogIndex      int
	PrevLogTerm       int
	Entries           []Entry
	LeaderCommitIndex int
}

// AppendEntriesReply structure for arguments
type AppendEntriesReply struct { //! modified
	Term    int
	ID      int
	Success bool
}

//  Method to send AppendEntries RPC a to server
func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	if ok {
		// if reply has a term > my term revert to follower
		rf.mu.Lock()
		if reply.Term > rf.currTerm && rf.me == rf.leaderID {
			//fmt.Printf("- %d REVERTING to follower\n", rf.me)
			// reset votedFor and set leader info
			rf.won = false
			rf.votes = 0
			rf.votedFor = -1
			rf.leaderID = -1
			rf.isCandidate = false
			rf.quitLeaderChan <- 0
			rf.mu.Unlock()
			return ok
		}
		//rf.mu.Unlock()

		// reply received = check if append was declined
		if !reply.Success {
			// if append entries is declined - decrement next index wait and wait for next heartbeat
			//rf.mu.Lock()

			rf.peersNextIndex[server]--
			rf.peersMatchIndex[server]--

			//rf.mu.Unlock()
		} else {
			// if append entries was successful (servers agree) update the match index to prevLogIndex and next index to + 1

			// if the nextIndex already match, success does not indicate a new entry being added.
			// But if the Followers next index is < leaders, this means that entry append was successful and we can incremetn next index
			if rf.peersNextIndex[server] < rf.peersNextIndex[rf.me] {
				//fmt.Printf("-- --- --- Before update was %d < %d (me)\n", rf.peersNextIndex[server], rf.peersNextIndex[rf.me])
				rf.peersNextIndex[server] = args.PrevLogIndex + 1 + len(args.Entries)
				rf.peersMatchIndex[server] = rf.peersNextIndex[server] - 1
				//fmt.Printf("* - - - - - - - Next Index updated for %d to -> %d\n", server, rf.peersNextIndex[server])
				//fmt.Printf("* - - - - - - - - - - - MATCH updated for %d to -> %d\n", server, rf.peersMatchIndex[server])
				//time.Sleep(20 * time.Microsecond)

			}
			//rf.mu.Unlock()
		}
		rf.mu.Unlock()

	} else {
		//fmt.Printf("-%d sees Server is down %d\n", args.ID, server)
	}

	return ok
}

// AppendEntries serves as an RPC handler for AppendEntries call
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) { //! modified for assignment 3

	// Ensure that leader is set correctly if the node is coming back up
	rf.mu.Lock()
	if rf.leaderID == -1 || rf.currTerm <= reply.Term {

		rf.leaderID = reply.ID
		rf.currTerm = reply.Term
	}
	if rf.commitIndex > 0 && rf.commitIndex < len(rf.myLog) && rf.currTerm < rf.myLog[rf.commitIndex].Term {
		rf.currTerm = rf.myLog[rf.commitIndex].Term
	}
	rf.mu.Unlock()
	// reset election timer by setting a new random timeout
	rf.appendEntriesChan <- args

	rf.mu.Lock()
	defer rf.mu.Unlock()
	currTerm := rf.currTerm
	//fmt.Printf("%d received term %d, my term is %d, commit index is %d, log length is %d\n", rf.me, args.Term, rf.currTerm, rf.commitIndex, len(rf.myLog))

	//set replies
	// False if term mismatch or prevIndex is not in log OR if term does not match for prev Index
	if args.Term < currTerm || args.PrevLogIndex > len(rf.myLog)-1 || (args.PrevLogIndex >= 1 && rf.myLog[args.PrevLogIndex].Term != args.PrevLogTerm) {
		//fmt.Printf("Checking index %d, my last index is %d, term at it is %d vs %d\n", args.PrevLogIndex, len(rf.myLog)-1, rf.myLog[args.PrevLogIndex].Term, args.PrevLogTerm)
		//fmt.Printf("Printing LOG : ")
		// for i := range rf.myLog {
		// 	fmt.Printf("%d ", rf.myLog[i].Term)
		// }
		//fmt.Printf("\n")
		reply.Success = false
		// Send updated term to 'leader' if it was behind
		if args.Term < currTerm {
			//fmt.Printf("* - * _ *_ OLD LEADER DUDE\n")
			reply.Term = currTerm
		}

		time.Sleep(400 * time.Microsecond)
		//fmt.Printf("%d at term %d mismatch with leader - %d, at index %d with len %d\n", rf.me, rf.currTerm, rf.leaderID, args.PrevLogIndex, len(rf.myLog))

		if args.Term >= currTerm && args.PrevLogIndex <= len(rf.myLog)-1 && rf.myLog[args.PrevLogIndex].Term != args.PrevLogTerm {
			//existing entry does not match with leaders, remove it and all that follow
			rf.myLog = rf.myLog[0:args.PrevLogIndex]

		}

	} else if args.PrevLogIndex == 0 || (args.PrevLogIndex > 0 && rf.myLog[args.PrevLogIndex].Term == args.PrevLogTerm) {

		//fmt.Printf("- %d SUCCESS append from leader %d at index %d\n", rf.me, args.ID, args.PrevLogIndex+1)
		reply.Success = true
		// discard rest of the log

		// check if entries are appended and is not duplicate
		if args.Entries != nil {
			// add only if prevLog + 1 (my next) is empty or has a mismatch
			if len(rf.myLog) == args.PrevLogIndex+1 {
				rf.myLog = append(rf.myLog, args.Entries...)
				//fmt.Printf("* - - - - - Entry added! by %d at index [%d], -> %d\n", rf.me, len(rf.myLog)-1, rf.myLog[len(rf.myLog)-1].Command)

			} else if len(rf.myLog) > args.PrevLogIndex+1 { // Otherwise if I have something else replace it

				rf.myLog = append(rf.myLog[0:args.PrevLogIndex+1], args.Entries...)
				//fmt.Printf("* - - - - - Entry replaced! by %d at index [%d], -> %d\n", rf.me, args.PrevLogIndex+1, rf.myLog[args.PrevLogIndex+1].Command)

			}
			go rf.persist()

		}
		if args.LeaderCommitIndex > rf.commitIndex {
			//fmt.Printf("* --- Follower commit updated by %d, from %d -> %d\n", rf.me, rf.commitIndex, int(math.Min(float64(args.LeaderCommitIndex), float64(len(rf.myLog)-1))))
			rf.commitIndex = int(math.Min(float64(args.LeaderCommitIndex), float64(len(rf.myLog)-1)))
			go rf.persist()

		}

	}

}

// Start explanation
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

	//make sure I am the leader

	rf.mu.Lock()

	index := len(rf.myLog)
	term := rf.currTerm
	isLeader := false

	// if I am the leader then make a new entry in my log
	if rf.leaderID == rf.me {
		isLeader = true
		//fmt.Printf("--LEADER %d with term %d Starting append at %d with %d\n", rf.me, rf.currTerm, index, command)
		//time.Sleep(20 * time.Microsecond)

		//make new entry
		newEntry := Entry{term, command}
		rf.myLog = append(rf.myLog, newEntry)
		rf.peersNextIndex[rf.me]++
		rf.peersMatchIndex[rf.me]++

		rf.mu.Unlock()
		rf.BroadCastAppends()
		rf.mu.Lock()

	}
	rf.mu.Unlock()

	return index, term, isLeader
}

// Kill explanation
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.quitChan <- 1
}

//BroadCastAppends sends out appends / heartbeats
func (rf *Raft) BroadCastAppends() {

	rf.mu.Lock()
	currTerm := rf.currTerm
	me := rf.me
	rf.mu.Unlock()

	for i := range rf.peers {

		if i != rf.me {

			//fmt.Printf(" * * * sending beat * to: %d, from %d\n", i, me)
			rf.mu.Lock()
			prevLogI := rf.peersNextIndex[i] - 1
			prevLogT := -1
			commitIndex := rf.commitIndex
			// if log is not empty then index to get the prevLogTerm, otherwise leave it at minus 1
			if prevLogI <= (len(rf.myLog)-1) && prevLogI >= 0 {
				// fmt.Printf("hit %d\n", prevLogI)
				prevLogT = rf.myLog[prevLogI].Term
			}

			// Send entries
			var entries []Entry
			entries = nil

			// if last log index >= nextIndex for a follower: send with log entries starting from next index
			//fmt.Printf("* * Sending append to %d, at index %d with term %d\n", i, prevLogI, prevLogT)
			if len(rf.myLog)-1 >= prevLogI+1 && prevLogI+1 >= 0 {
				entries = append(entries, rf.myLog[prevLogI+1:]...)
				//time.Sleep(10 * time.Microsecond)

				//fmt.Printf(" --- -- Setting entry: %d\n", rf.myLog[prevLogI+1].Term)
				//fmt.Printf("* - - - %d at term %d Sending append to %d, at index %d with %d\n", rf.me, rf.currTerm, i, prevLogI+1, entries[0].Command)
			}

			args := AppendEntriesArgs{currTerm, me, prevLogI, prevLogT, entries, commitIndex}
			reply := AppendEntriesReply{currTerm, me, false}
			rf.mu.Unlock()
			go rf.sendAppendEntries(i, args, &reply)

		}

	}
	go rf.persist()
}

// Leader routine
//* Leader routine
func (rf *Raft) Leader() {

	//fmt.Printf("leader started\n")
	rf.mu.Lock()
	//fmt.Printf("--I AM THE LEADER %d for term %d\n", rf.me, rf.currTerm)

	// set next indicies and match indicies
	for i := range rf.peers {
		// for each peer

		rf.peersMatchIndex[i] = 0
		rf.peersNextIndex[i] = len(rf.myLog)

	}
	rf.peersMatchIndex[rf.me] = len(rf.myLog) - 1

	rf.mu.Unlock()

	timer := rf.heartbeatFrequency

	for {
		select {
		case <-rf.quitLeaderChan:
			rf.mu.Lock()
			//fmt.Printf("Quiting leader routine %d\n", rf.me)
			rf.heartbeatTimer = 0
			rf.won = false
			rf.votes = 0
			// reset votedFor and set leader info
			rf.votedFor = -1
			rf.leaderID = -1
			rf.isCandidate = false
			rf.mu.Unlock()
			return
		default:

			rf.mu.Lock()
			newCommit := rf.commitIndex
			count := 0
			// count values which are bigger than old CommitIndex
			// find the smallest value which is bigger than old CommitIndex
			for i := range rf.peersMatchIndex {
				if rf.peersMatchIndex[i] > rf.commitIndex {
					count++
					if newCommit == rf.commitIndex || newCommit > rf.peersMatchIndex[i] {
						newCommit = rf.peersMatchIndex[i]
					}
				}
			}
			//fmt.Printf("count : %f  proportion : %f\n", float32(count), float32(len(rf.peers))/2.0)
			if float32(count) > float32(len(rf.peers))/2.0 && rf.me == rf.leaderID {
				//fmt.Printf("New commit it %d\n", newCommit)
				//time.Sleep(10 * time.Microsecond)

				rf.commitIndex = newCommit
				go rf.persist()

			}
			rf.mu.Unlock()

			if timer >= rf.heartbeatFrequency {
				// Send heartbeat to everyone

				timer = 0

				go rf.BroadCastAppends()

			}

			timer++
			time.Sleep(1 * time.Millisecond)

		}

	}
}

// Election routine
//* Election routine
func (rf *Raft) Election() {
	//fmt.Printf("%d Timed out starting elections\n", rf.me)
	rf.mu.Lock()

	rf.votes = 1
	// ask for votes
	rf.heartbeatTimer = 0
	rf.timeoutElection = rand.Intn(400-200) + 200

	prevLogI := len(rf.myLog) - 1
	prevLogT := -1
	if prevLogI >= 0 {
		prevLogT = rf.myLog[prevLogI].Term
	}

	args := RequestVoteArgs{rf.currTerm, rf.me, prevLogI, prevLogT}
	rf.won = false
	rf.mu.Unlock()

	for i := range rf.peers {
		if i != rf.me {

			//fmt.Printf("%d requsted vote from %d\n", rf.me, i)

			reply := RequestVoteReply{-1, false}
			go rf.sendRequestVote(i, args, &reply)

		}
	}

}

//TickTok will increment the heatbeattimer
func (rf *Raft) TickTok() {
	for {
		time.Sleep(1 * time.Millisecond)
		rf.mu.Lock()
		if rf.me != rf.leaderID {
			rf.heartbeatTimer++
		} else {
			rf.heartbeatTimer = 0
		}
		rf.mu.Unlock()
	}
}

// Run starts the server/node in follower mode with the timeouts etc
// set,
//* Run is the MAIN follower Go routine for Raft
func (rf *Raft) Run() {

	// infinite loop that sleeps for timing
	for {
		select {
		case reply := <-rf.appendEntriesChan:
			rf.mu.Lock()
			// in case append entry is received, reset the timer
			//fmt.Printf("%d 's term is %d", rf.me, rf.currTerm)

			//fmt.Printf("reply term is %d\n", reply.Term)

			if reply.Term >= rf.currTerm || rf.leaderID == -1 {
				//fmt.Printf(" * * * %d got at term %d beat from Leader %d\n", rf.me, rf.currTerm, reply.ID)

				rf.heartbeatTimer = 0

				if rf.leaderID == rf.me {
					rf.quitLeaderChan <- 0

				}
				//fmt.Printf(" * * %d got at term %d beat from Leader %d **** No block\n", rf.me, rf.currTerm, reply.ID)

				// reset votedFor and set leader info
				rf.votedFor = -1
				rf.won = false
				rf.votes = 0
				rf.leaderID = reply.ID
				rf.currTerm = reply.Term
				rf.isCandidate = false
			}

			rf.mu.Unlock()

		case <-rf.quitChan:
			rf.quitLeaderChan <- 1
			return

		default:

			rf.mu.Lock()
			if rf.heartbeatTimer >= rf.timeoutElection && rf.me != rf.leaderID {
				//fmt.Printf("%d timed out at time: %d changing to candidate at term %d, leader was %d\n", rf.me, rf.heartbeatTimer, rf.currTerm, rf.leaderID)
				rf.heartbeatTimer = 0
				rf.currTerm++
				//timeout has occured, go into candidate mode
				rf.isCandidate = true
				// vote for myself
				rf.votedFor = rf.me
				rf.leaderID = -1
				rf.mu.Unlock()
				go rf.Election()
				rf.mu.Lock()

			}
			rf.mu.Unlock()

			time.Sleep(1 * time.Millisecond)

		}
	}

}

//ApplyToStateMachine will be a go routine to apply messages to the statemachine
func (rf *Raft) ApplyToStateMachine(applyChan chan ApplyMsg) {

	for {
		rf.mu.Lock()

		if rf.commitIndex > rf.lastAppliedIndex && rf.lastAppliedIndex+1 < len(rf.myLog) {
			rf.lastAppliedIndex++

			applyChan <- ApplyMsg{rf.lastAppliedIndex, rf.myLog[rf.lastAppliedIndex].Command, false, nil}
			time.Sleep(10 * time.Microsecond)
			//fmt.Printf("* - - - - - - - - - Applied by %d at index [%d] -> %d\n", rf.me, rf.lastAppliedIndex, rf.myLog[rf.lastAppliedIndex].Command)

		}
		rf.mu.Unlock()
		time.Sleep(1 * time.Millisecond)
	}

}

// Make explanation
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

	//! Assignment 3
	rf.emptyValue = Entry{-99, nil}
	rf.myLog = append(rf.myLog, rf.emptyValue)
	rf.lastAppliedIndex = 0
	rf.commitIndex = 0
	rf.peersNextIndex = make([]int, len(rf.peers))
	rf.peersMatchIndex = make([]int, len(rf.peers))

	// Your initialization code here.
	//time.Sleep(1 * time.Millisecond)
	rand.Seed(time.Now().UnixNano())
	rf.currTerm = 0
	rf.heartbeatFrequency = 100
	rf.heartbeatTimer = 0

	rf.leaderID = -1
	rf.votedFor = -1
	rf.isCandidate = false
	rf.timeoutElection = rand.Intn(400-200) + 200
	rf.appendEntriesChan = make(chan AppendEntriesArgs, 1000)
	rf.requestVoteChan = make(chan RequestVoteReply, 1000)
	rf.quitChan = make(chan int, 1000)
	rf.quitLeaderChan = make(chan int, 1000)
	rf.won = false

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	//fmt.Printf("Starting with %d\n", rf.timeoutElection)

	go rf.Run()
	go rf.TickTok()
	go rf.ApplyToStateMachine(applyCh)

	return rf
}

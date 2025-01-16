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
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 3A
	currentTerm int
	votedFor    int
	Logs        []Log

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	// custom
	role          int           // 记录节点角色
	heartbeatCh   chan int      // 捕获心跳
	electionCh    chan struct{} // 捕获选举成功
	voteCnt       int           // 得票数
	shouldVoteCnt int           // 理应得票数

	applyCh chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	// DPrintf("get server [%d] state, result: [%v], term: [%v]", rf.me, rf.role, rf.currentTerm)
	return rf.currentTerm, rf.role == RaftRoleLeader
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// if candidate as up-to-date as rf return true
func (rf *Raft) compareEntriesWithCandidate(cLastLogIndex, cLastLogTerm int) bool {
	if cLastLogTerm < rf.getLastLogTerm() {
		return false
	}
	if cLastLogTerm > rf.getLastLogTerm() {
		return true
	}
	if cLastLogIndex >= rf.getLastLogIndex() {
		return true
	}
	return false
}

// RequestVote RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.checkTerm(args.Term)
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	// figure2 rule2
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.compareEntriesWithCandidate(args.LastLogIndex, args.LastLogTerm) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.checkTerm(reply.Term)
	if ok && reply.VoteGranted {
		//DPrintf("server [%d] send vote to [%d], result: [%v], term: [%v]", rf.me, server, reply.VoteGranted, rf.currentTerm)
		rf.voteCnt++
	}
	if rf.voteCnt >= rf.shouldVoteCnt {
		//DPrintf("server [%d] voteCnt >= 2, term: [%v]", rf.me, rf.currentTerm)
		select {
		case rf.electionCh <- struct{}{}:
		default:
		}
		rf.voteCnt = 0
	}
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// DPrintf("server [%d] send heartbeat to [%d], term: [%v]", rf.me, server, rf.currentTerm)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.checkTerm(reply.Term)
	return ok
}

// 校验RPC request and response term true: change follower
func (rf *Raft) checkTerm(term int) bool {
	if term > rf.currentTerm {
		// DPrintf("server [%d] check term role [%v]\n", rf.me, rf.role)
		rf.currentTerm = term
		rf.votedFor = -1
		rf.voteCnt = 0
		rf.changeFollower()
		return true
	}
	return false
}

// AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.checkTerm(args.Term)
	reply.Term = rf.currentTerm
	if args.Term >= rf.currentTerm {
		// reply.Success = true
		rf.role = RaftRoleFollower
	}

	// figure2 rule1
	if args.Term < rf.currentTerm {
		return
	}
	select {
	case rf.heartbeatCh <- args.Term:
	default:
	}

	// figure rule5
	if args.LeaderCommit > rf.commitIndex {
		oldCommitIndex := rf.commitIndex
		rf.commitIndex = min(args.LeaderCommit, len(rf.Logs))
		// 提交日志
		if oldCommitIndex < rf.commitIndex {
			for i := oldCommitIndex + 1; i <= rf.commitIndex; i++ {

				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      rf.Logs[i-1].Command,
					CommandIndex: i,
				}
				// DPrintf("server %d role: %d commit log %d command: %v", rf.me, rf.role, i, rf.Logs[i-1].Command)
			}
		}
	}

	// handle replicate
	if len(args.Entries) != 0 {
		// 首條日志
		if args.PrevLogIndex == 0 && args.PrevLogTerm == 0 {
			reply.Success = true
			// 清空衝突的
			if len(rf.Logs) != 0 {
				rf.Logs = make([]Log, 0)
			}
			// 添加新日志
			rf.Logs = append(rf.Logs, args.Entries...)
			return
		}

		// 非首條日志 對比 prevLogIndex 和 prevLogTerm
		if len(rf.Logs) < args.PrevLogIndex {
			return
		}
		// figure rule2 rule3
		if rf.Logs[args.PrevLogIndex-1].Term != args.PrevLogTerm {
			rf.Logs = rf.Logs[:args.PrevLogIndex-1]
			return
		}
		// figure rule4
		reply.Success = true
		rf.Logs = append(rf.Logs, args.Entries[args.PrevLogIndex:]...)
	}
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
	// Your code here (3B).
	if !rf.Leader() {
		return 0, 0, false
	}
	// append entry to local log
	rf.mu.Lock()
	rf.Logs = append(rf.Logs, Log{
		Index:   len(rf.Logs) + 1,
		Command: command,
		Term:    rf.currentTerm,
	})
	rf.mu.Unlock()

	// log replicate
	rf.replicate()
	return len(rf.Logs), rf.currentTerm, true
}

func (rf *Raft) replicate() {
	var wg sync.WaitGroup
	wg.Add(rf.shouldVoteCnt)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(i int) {
			args := &AppendEntriesArgs{}
			reply := &AppendEntriesReply{}
			prevLogIndex := rf.nextIndex[i]
			for !reply.Success {
				prevLogTerm := 0
				if prevLogIndex > 0 {
					prevLogTerm = rf.Logs[prevLogIndex-1].Term
				}
				args = &AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					Entries:      rf.Logs,
					LeaderCommit: rf.commitIndex,
				}
				reply = &AppendEntriesReply{}
				rf.sendAppendEntries(i, args, reply)
				prevLogIndex--
			}
			rf.nextIndex[i] = len(rf.Logs) + 1
			rf.matchIndex[i] = len(rf.Logs)
			wg.Done()
		}(i)
	}
	go func() {
		wg.Wait()
		rf.mu.Lock()
		defer rf.mu.Unlock()
		// 提交日志
		if len(rf.Logs) > rf.commitIndex {
			for i := rf.commitIndex + 1; i <= len(rf.Logs); i++ {

				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      rf.Logs[i-1].Command,
					CommandIndex: i,
				}
				// DPrintf("server %d role: %d commit log %d command: %v", rf.me, rf.role, i, rf.Logs[i-1].Command)
			}
			rf.commitIndex = len(rf.Logs)
		}
	}()
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

func (rf *Raft) changeFollower() {
	rf.role = RaftRoleFollower
}

func (rf *Raft) lead() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.role = RaftRoleLeader

	// reinitialized after election
	rf.nextIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.getLastLogIndex() + 1
	}
	rf.matchIndex = make([]int, len(rf.peers))

	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(i int) {
			for {
				rf.mu.RLock()
				if rf.role != RaftRoleLeader {
					rf.mu.RUnlock()
					break
				}
				rf.mu.RUnlock()
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					LeaderCommit: rf.commitIndex,
				}
				reply := AppendEntriesReply{}
				go rf.sendAppendEntries(i, &args, &reply)
				time.Sleep(time.Duration(100) * time.Millisecond)
			}
		}(i)
	}
}

func (rf *Raft) elect() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.role = RaftRoleCandidate
	rf.voteCnt = 1 // 自己的一票
	rf.votedFor = rf.me
	rf.currentTerm++

	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(i int) {
			for rf.role == RaftRoleCandidate {
				args := RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateId:  rf.me,
					LastLogIndex: rf.getLastLogIndex(),
					LastLogTerm:  rf.getLastLogTerm(),
				}
				reply := RequestVoteReply{}
				go rf.sendRequestVote(i, &args, &reply)
				time.Sleep(time.Duration(100) * time.Millisecond)
			}
			// DPrintf("server [%d] send request vote [%v]\n", rf.me, reply.VoteGranted)
		}(i)
	}
}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.Logs)
}

func (rf *Raft) getLastLogTerm() int {
	if len(rf.Logs) == 0 {
		return 0
	}
	return rf.Logs[len(rf.Logs)-1].Term
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here (3A)
		// Check if a leader election should be started.
		if rf.role == RaftRoleLeader {
			continue
		}
		ms := 200 + (rand.Int63() % 200)
		select {
		case <-rf.electionCh:
			// 选举成功
			// DPrintf("server [%d] election success term: [%d]\n", rf.me, rf.currentTerm)
			rf.lead()
		case <-rf.heartbeatCh:
			// 收到心跳 转为follower
			// DPrintf("server [%d] receive heartbeat \n", rf.me)
		case <-time.After(time.Duration(ms) * time.Millisecond):
			// 没有心跳 发起选举
			// DPrintf("server [%d] change candidate \n", rf.me)
			rf.elect()
		}
	}
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here (3A, 3B, 3C).
	// 3A
	rf.votedFor = -1 // -1代表null 当前任期没有投票
	rf.heartbeatCh = make(chan int, 1)
	rf.electionCh = make(chan struct{}, 1)
	rf.shouldVoteCnt = len(peers)/2 + 1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func (rf *Raft) Leader() bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.role == RaftRoleLeader
}

func (rf *Raft) Follower() bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.role == RaftRoleFollower
}

func (rf *Raft) Candidate() bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.role == RaftRoleCandidate
}

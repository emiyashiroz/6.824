package raft

// 需要对外暴露的接口
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

// ApplyMsg in part 2D you'll want to send other kinds of messages (e.g.,
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
}

// Raft A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Persistent state
	currentTerm int        // 当前任期
	votedFor    int        // 当前任期投票给谁
	logs        []LogEntry // 日志
	// Volatile state for all
	commitIndex int // 已提交索引
	lastApplied int // 被应用到状态机的索引
	// Volatile state for leader
	nextIndex  []int
	matchIndex []int
	// customed
	curRole int    // 当前角色 0: Follower; 1: Leader; 2: Candidate
	heart   bool   // 是否收到心跳
	elect   bool   // 判断是否开启选举
	voteRes []bool // 收集选票的通道
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.curRole == 1
	return term, isleader
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

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// RequestVoteArgs example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // 候选者任期号
	CandidateId  int // 候选者id
	lastLogIndex int
	lastLogTerm  int
}

// RequestVoteReply example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

type AppendEntriesArgs struct {
	Term         int // leader's term
	LeaderId     int // leaderId
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int // leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

// AppendEntries handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// 收到了来自leader的心跳
	if args.Term >= rf.currentTerm {
		rf.heart = true
		rf.curRole = 0
		rf.currentTerm = args.Term
		return
	}
	reply.Term = rf.currentTerm
}

func (rf *Raft) SendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	// 发现其他节点的term已经大于该leader节点的term, 回退到follower
	if reply.Term > rf.currentTerm {
		rf.curRole = 0
	}
	return ok
}

// RequestVote RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// 投票规则 1.首先判断currentTerm是不是大于自己的, 不是直接拒绝
	// 2.是的话看更新
	//fmt.Printf("%d号服务器收到来自%d号服务器的投票请求\n", rf.me, args.CandidateId)
	if args.Term <= rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	//fmt.Printf("%d号服务器投票给%d号服务器\n", rf.me, args.CandidateId)
	rf.currentTerm = args.Term
	reply.Term = args.Term
	reply.VoteGranted = true
	rf.votedFor = args.CandidateId
	return

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
	//fmt.Printf("%d号服务器向%d号服务器发送投票请求\n", rf.me, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if reply.VoteGranted {
		rf.voteRes[server] = true
	}
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.

// Start the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.

// Kill the issue is that long-running goroutines use memory and may chew
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

func (rf *Raft) ticker() {
	for rf.killed() == false {
		switch rf.curRole {
		case 0:
			rf.follower()
		case 1:
			rf.leader()
		case 2:
			rf.candidate()
		default:
			time.Sleep(1)
		}
	}
}

func (rf *Raft) follower() {
	rf.heart = false
	//fmt.Println(rf.me, ": ", "开始心跳超时")
	time.Sleep(time.Duration(150) * time.Millisecond)
	// 心跳判断
	if rf.curRole == 0 && !rf.heart {
		rf.curRole = 2 // 转候选者
	}
}

func (rf *Raft) leader() {
	// Leader发送心跳
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		args := &AppendEntriesArgs{
			Term:     rf.currentTerm,
			LeaderId: rf.me,
		}
		reply := &AppendEntriesReply{}
		go rf.SendAppendEntries(i, args, reply) // 这里如果reply的term大于自己的term会回到follower
	}
	time.Sleep(time.Duration(150) * time.Millisecond)
}

func (rf *Raft) candidate() {
	// rf.chVotes = make(chan bool, len(rf.peers))
	//fmt.Println(rf.me, ": ", "开始选举")
	votes := 0 // 选票数
	rf.votedFor = rf.me
	rf.currentTerm = rf.currentTerm + 1
	args := &RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}
	reply := &RequestVoteReply{}
	rf.voteRes = make([]bool, len(rf.peers))
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.sendRequestVote(i, args, reply)
	}
	// 选举超时时间350-650ms
	ms := 150 + (rand.Int63() % 200)
	time.Sleep(time.Duration(ms) * time.Millisecond)
	// 如果这期间收到了大于等于节点currentTerm的心跳
	if rf.curRole == 0 {

		//fmt.Println(rf.me, ": ", "收到了大于等于节点currentTerm的心跳")
		return
	}
	// 判断选票
	for _, v := range rf.voteRes {
		// //fmt.Println(rf.me, ": ", "得到一票")
		if v {
			votes++
		}
	}
	//fmt.Println(rf.me, ": ", "得到", votes+1, "票")
	// 当选, 否则继续选举
	if votes+1 > (len(rf.peers)-1)/2 {
		rf.curRole = 1
	}
}

// Make the service or tester wants to create a Raft server. the ports
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

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.commitIndex = 0
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// rf.chVotes = make(chan bool, len(rf.peers))
	rf.votedFor = -1
	rf.elect = false
	rf.curRole = 0
	rf.heart = false
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

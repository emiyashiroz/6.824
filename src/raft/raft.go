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
	Index   int
	Term    int
	Command interface{}
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
	cntVoted    int        // 票数
	logs        []LogEntry // 日志
	// Volatile state for all
	commitIndex int // 已提交索引
	lastApplied int // 被应用到状态机的索引
	// Volatile state for leader
	nextIndex  []int
	matchIndex []int
	// customed
	curRole          int           // 当前角色 0: Follower; 1: Leader; 2: Candidate
	heartCh          chan struct{} // 心跳信号
	voted            chan struct{} // 投票信号
	winElect         chan struct{} // 选举成功信号
	leaderToFollower chan struct{} // leader转follower信号
	candiToFollower  chan struct{} // candidate转follower信号
	appendEntriesRes []bool        // 日志复制结果
	applyCh          chan ApplyMsg
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.curRole == RaftRoleLeader
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

func (rf *Raft) setCandiToFollower() {
	go func() {
		rf.candiToFollower <- struct{}{}
	}()
}

func (rf *Raft) setLeaderToFollower() {
	go func() {
		rf.leaderToFollower <- struct{}{}
	}()
}

func (rf *Raft) setHeartBeat() {
	go func() {
		rf.heartCh <- struct{}{}
	}()
}

func (rf *Raft) setWinElect() {
	go func() {
		rf.winElect <- struct{}{}
	}()
}

func (rf *Raft) setVoted() {
	go func() {
		rf.voted <- struct{}{}
	}()
}

func (rf *Raft) changeRole(role int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.curRole == role {
		return
	}
	switch role {
	case RaftRoleFollower:
		rf.curRole = RaftRoleFollower
	case RaftRoleCandidate:
		rf.curRole = RaftRoleCandidate
	case RaftRoleLeader:
		rf.curRole = RaftRoleLeader
	default:
		panic("Unknown role")
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.curRole != RaftRoleCandidate {
		return
	}
	rf.votedFor = rf.me
	rf.cntVoted = 1
	rf.currentTerm += 1
	args := RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.sendRequestVote(i, &args, &RequestVoteReply{})
	}
}

func (rf *Raft) broadCastAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.curRole != RaftRoleLeader {
		return
	}
	args := AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
	}
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.SendAppendEntries(i, &args, &AppendEntriesReply{})
	}
}

// AppendEntries handler rpc server endpoint
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// 收到了来自leader的心跳
	// 心跳逻辑
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Entries == nil {
		if args.Term < rf.currentTerm {
			reply.Success = false
			return
		}
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.votedFor = StatusNotVoted
			if rf.curRole == RaftRoleCandidate {
				rf.setCandiToFollower()
			} else if rf.curRole == RaftRoleLeader {
				rf.setLeaderToFollower()
			}
		}
		rf.setHeartBeat()
		return
	}
	// logEntries 逻辑5.1
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	reply.Term = args.Term
	// 5.3 没有preLogIndex这条Log
	if len(rf.logs) < args.PrevLogIndex {
		reply.Success = false
		return
	}
	// 5.3 存在但是term不一致 // 要删除这之后的
	if args.PrevLogIndex != 0 && rf.logs[args.PrevLogIndex-1].Term != args.PrevLogTerm {
		rf.logs = rf.logs[:args.PrevLogIndex-1]
		reply.Success = false
		return
	}
	// 一致, 更新commitIndex
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.logs))
	}
	// 一致, 复制prevLogIndex后面所有的
	rf.logs = append(rf.logs, args.Entries[args.PrevLogIndex:]...)
	rf.commitIndex = len(rf.logs)
	for i := args.PrevLogIndex + 1; i <= len(rf.logs); i++ {
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[i-1].Command,
			CommandIndex: i,
		}
		// fmt.Println("follower send applyMsg:", applyMsg.Command, applyMsg.CommandIndex)
		rf.applyCh <- applyMsg
	}
	reply.Success = true
}

func (rf *Raft) SendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//if len(args.Entries) > 0 {
	//	fmt.Printf("SendAppendEntries leader: %d, follower: %d, entries: %d, cmd: %v\n", rf.me, server, len(args.Entries), args.Entries[len(args.Entries)-1].Command)
	//} else {
	//	fmt.Printf("SendAppendEntries leader: %d, follower: %d, entries: %d\n", rf.me, server, len(args.Entries))
	//}
	// ---------------------------这是心跳-------------------------------- //
	if args.Entries == nil {
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		// 发现其他节点的term已经大于该leader节点的term, 回退到follower 这里需要有额外逻辑了 通过TestFailAgree2B测试
		if !ok {
			return
		}
		rf.mu.Lock()
		defer rf.mu.Unlock()
		//if rf.curRole != RaftRoleLeader || rf.currentTerm > reply.Term {
		//	return
		//}
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.votedFor = StatusNotVoted
			rf.setLeaderToFollower()
		}
		return
	}
	// ------------------------日志复制不成功重试-------------------------- //
	for reply.Success == false {
		args.PrevLogIndex = rf.nextIndex[server]
		if args.PrevLogIndex <= 0 {
			args.PrevLogTerm = rf.currentTerm
		} else {
			args.PrevLogTerm = rf.logs[args.PrevLogIndex-1].Term
		}
		rf.peers[server].Call("Raft.AppendEntries", args, reply)
		if reply.Success == false {
			rf.nextIndex[server] = rf.nextIndex[server] - 1
		}
		time.Sleep(time.Duration(150) * time.Millisecond)
	}
	rf.nextIndex[server] = len(rf.logs)
	rf.matchIndex[server] = len(rf.logs) - 1
	rf.appendEntriesRes[server] = true
	return
}

// RequestVote RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	//NewVoter(-1, rf).HandleRequestVote(args, reply)
	rf.mu.Lock()
	//fmt.Printf("%d号ServerRequestVote获得锁\n", rf.me)
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		if rf.curRole == RaftRoleCandidate {
			///fmt.Printf("%d号, args.Term:%d > rf.currentTerm:%d", rf.me, args.Term, rf.currentTerm)
			rf.setCandiToFollower()
		} else if rf.curRole == RaftRoleLeader {
			rf.setLeaderToFollower()
		}
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if rf.votedFor == StatusNotVoted || rf.votedFor == args.CandidateId {
		//fmt.Printf("%d号给%d号候选者投票\n", rf.me, args.CandidateId)
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.setVoted()
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.curRole != RaftRoleCandidate || rf.currentTerm != args.Term {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.votedFor = StatusNotVoted
		rf.setCandiToFollower()
	}
	if reply.VoteGranted {
		rf.cntVoted += 1
		if rf.cntVoted > len(rf.peers)/2 {
			rf.setWinElect()
		}
	}
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
	isLeader = rf.curRole == RaftRoleLeader
	// 该节点非leader, todo: followerStart
	if !isLeader {
		return index, term, isLeader
	}
	// leader节点处理
	return rf.LeaderStart(command)
}

func (rf *Raft) LeaderStart(command interface{}) (int, int, bool) {
	// 添加到日志
	rf.logs = append(rf.logs, LogEntry{
		Index:   len(rf.logs),
		Term:    rf.currentTerm,
		Command: command,
	})
	reply := &AppendEntriesReply{}
	// 重置日志复制结果
	for i, _ := range rf.appendEntriesRes {
		rf.appendEntriesRes[i] = false
	}
	// 发送日志
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		// 对每个follower要根据matchindex来设置args中的entries参数, 通过bytescount检查, 这里将不需要的command设置为nil
		sendArgs := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			Entries:      rf.logs,
			LeaderCommit: rf.commitIndex,
		}
		// 减少传输量通过RPCBytes测试
		for j := 0; j <= rf.matchIndex[i]; j++ {
			sendArgs.Entries[j].Command = nil
		}
		go rf.SendAppendEntries(i, sendArgs, reply)
	}
	res := 0
	// 死循环了 设置超时次数
	n := 0
	for res <= (len(rf.peers)-1)/2 && n < 1000 {
		res = 0
		n++
		time.Sleep(time.Duration(10) * time.Millisecond)
		for i, _ := range rf.appendEntriesRes {
			if rf.appendEntriesRes[i] {
				res++
			}
		}
		time.Sleep(1)
	}
	if res <= (len(rf.peers)-1)/2 {
		return len(rf.logs), rf.currentTerm, true
	}
	applyMsg := ApplyMsg{
		CommandValid: true,
		Command:      command,
		CommandIndex: len(rf.logs),
	}
	rf.applyCh <- applyMsg
	rf.commitIndex = applyMsg.CommandIndex
	return len(rf.logs), rf.currentTerm, true
}

// leader节点才能调用
func (rf *Raft) getPrevLogTerm() int {
	if len(rf.logs) <= 1 {
		return rf.currentTerm
	}
	return rf.logs[len(rf.logs)-2].Term
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
		for {
			rf.mu.Lock()
			role := rf.curRole
			rf.mu.Unlock()
			switch role {
			case RaftRoleFollower:
				select {
				case <-rf.voted:
				case <-rf.heartCh:
				case <-time.After(RandTime(MinTimeout, MaxTimeout)):
					rf.changeRole(RaftRoleCandidate)
					go rf.startElection()
				}
			case RaftRoleLeader:
				select {
				case <-rf.leaderToFollower:
					rf.changeRole(RaftRoleFollower)
				case <-time.After(60 * time.Millisecond):
					go rf.broadCastAppendEntries()
				}
			case RaftRoleCandidate:
				select {
				case <-rf.winElect:
					rf.changeRole(RaftRoleLeader)
					go rf.broadCastAppendEntries()
				case <-rf.candiToFollower:
					rf.changeRole(RaftRoleFollower)
				case <-rf.heartCh:
					rf.changeRole(RaftRoleFollower)
				case <-time.After(RandTime(MinTimeout, MaxTimeout)):
					go rf.startElection()
				}
			default:
				time.Sleep(1)
			}
		}
	}
}

func (rf *Raft) initNextIndex() {
	for i, _ := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.logs)
	}
}

func (rf *Raft) initMatchIndex() {
	for i, _ := range rf.matchIndex {
		rf.matchIndex[i] = len(rf.logs) - 1
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
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.appendEntriesRes = make([]bool, len(peers))
	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.applyCh = applyCh
	rf.readPersist(persister.ReadRaftState())
	rf.votedFor = StatusNotVoted
	rf.curRole = RaftRoleFollower
	rf.heartCh = make(chan struct{})
	rf.leaderToFollower = make(chan struct{})
	rf.candiToFollower = make(chan struct{})
	rf.winElect = make(chan struct{})
	rf.voted = make(chan struct{})
	rf.cntVoted = 1
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

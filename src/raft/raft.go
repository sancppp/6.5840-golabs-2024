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

	"bytes"
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
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

const HeartBeatTimeoutMs = 100               //心跳超时，要求1秒10次，所以是100ms一次
const VoteTimeoutMs = HeartBeatTimeoutMs * 7 // 选举超时，远大于心跳超时

type ROLE string

const (
	FOLLOWER  ROLE = "Follower"
	CANDIDATE ROLE = "Candidate"
	LEADER    ROLE = "Leader"
)

// 日志条目结构体
type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	logger log.Logger

	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role          ROLE         //当前状态
	currentTerm   int          //currentTerm latest term server has seen (initialized to 0 on first boot, increases monotonically)
	currentLeader int          //当前的leader id
	votedFor      int          //candidateId that received vote in current term (or null if none)
	log           []*LogEntry  //log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
	voteCount     atomic.Int32 //选票数
	voteEvent     atomic.Bool
	appendEvent   atomic.Bool

	//所有机器的可变状态

	commitIndex atomic.Int32 //将被提交的日志记录的索引(初值为 0 且单调递增) 不需要持久化。
	lastApplied atomic.Int32 //已经被提交到状态机的最后一个日志的索引(初值为 0 且单调递增)。
	applyCh     chan ApplyMsg

	//作为leader需要管理其他节点的进度

	nextIndex []int //for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)

	matchIndex []int //for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	// 新上任的leader不能毫无缘由的（没有处理新的log）提交之前任期（非本任期）的日志。
	//ok==true说明这个leader在当前任期完成过一次请求，可以提交了
	ok bool

	//3D snapshot
	snapshot          []byte
	lastIncludedIndex int // 加入快照之后，index会有偏移量，lastIncludedIndex之前的logentry都被删除了
	lastIncludedTerm  int
}

func (rf *Raft) DPrintln(o bool, a ...interface{}) {
	if o {
		var aa []interface{}
		aa = append(aa,
			"id:", rf.me,
			"term:", rf.currentTerm,
			"leader:", rf.currentLeader,
			"role:", rf.role,
			a)
		rf.logger.Println(aa...)
	}
}

// 加入快照机制之后，与log有关的切片下标都应该减去偏移量&裁去前面
// 将原始的index（从0开始单调递增）转化为数组中的真正index（考虑偏移量）
// 若返回小于零则表示这个index已经被丢弃了
func (rf *Raft) getIndex(index int) int {
	return index - rf.lastIncludedIndex - 1
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == LEADER
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
// 调用方加锁
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)

	//3C: 哪些字段需要被持久化？论文中的图2
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	for index, log := range rf.log {
		rf.DPrintln(CDebug, "persist ", index, *log)
	}
	rf.DPrintln(CDebug, "persist ", rf.commitIndex.Load())
	raftstate := w.Bytes()
	//3C:For now, pass nil as the second argument to persister.Save()
	rf.persister.Save(raftstate, rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
	d.Decode(&rf.lastIncludedIndex)
	d.Decode(&rf.lastIncludedTerm)
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	//应用层定期调用这个方法，创建一个状态机快照
	//raft层应该丢弃index（含）之前的logentry，

	// 下标偏移1
	index -= 1
	rf.DPrintln(DDebug, "Snapshot:[ 0 :", index, "]")
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.snapshot = snapshot
	pre := rf.lastIncludedIndex
	rf.lastIncludedTerm = rf.log[rf.getIndex(index)].Term
	rf.lastIncludedIndex = index

	//裁剪切片
	rf.log = rf.log[rf.lastIncludedIndex-pre:]
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
// 投票请求
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int //candidate’s term
	CandidateId  int //candidate requesting vote
	LastLogTerm  int //term of candidate’s last log entry (§5.4)
	LastLogIndex int //index of candidate’s last log entry (§5.4)
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int  //currentTerm, for candidate to update itself
	VoteGranted bool //true means candidate received vote
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).

	if args.CandidateId == -1 {
		return
	}

	rf.mu.Lock()

	defer rf.mu.Unlock()
	defer rf.persist()
	// 1. 如果 leader 的任期小于自己的任期返回 false。(5.1)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		//更新自己的任期,switch role to follower
		rf.currentTerm = args.Term
		rf.switchRoleTo(FOLLOWER)
	}
	// 2. 如果本地 voteFor 为空，候选者日志和本地日志相同，则投票给该候选者 (5.2 和 5.4)

	lastLogTerm := 0 //获取当前节点的最后一条日志的任期
	if len(rf.log) != 0 {
		lastLogTerm = rf.log[len(rf.log)-1].Term
	} else if rf.lastIncludedIndex != -1 {
		//3D，日志被压缩完了
		lastLogTerm = rf.lastIncludedTerm
	}
	len_log := rf.lastIncludedIndex + 1 + len(rf.log) //3D,算上被压缩的
	ok := (args.LastLogTerm > lastLogTerm) ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex+1 >= len_log)
	rf.DPrintln(BDebug, "myself:", lastLogTerm, len(rf.log)-1)
	rf.DPrintln(BDebug, "candidate:", args.LastLogTerm, args.LastLogIndex)
	rf.DPrintln(BDebug, ok)
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && ok {
		rf.DPrintln(DDebug, rf.currentTerm, "投票给了 ", args.CandidateId)
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		rf.votedFor = args.CandidateId
		rf.voteEvent.Store(true) //只有投出了自己在这个任期内的选票，才刷新选举计时器
	} else {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
	}
}

// Leader不断判断是否有log已经超过半数了，更新commitIndex
func (rf *Raft) updateCommitIndex() {

	N := int(rf.commitIndex.Load()) + 1

	for _, isleader := rf.GetState(); isleader && !rf.killed(); _, isleader = rf.GetState() {
		if rf.getIndex(N) < 0 {
			//3D 如果第N条日志已经被丢弃了，则将N置为此时real下标为0
			N = rf.lastIncludedIndex + 1
		}
		cnt := 1
		for index := range rf.peers {
			if index == rf.me {
				continue
			}
			if rf.matchIndex[index] >= N {
				cnt++
			}
		}
		if cnt*2 < len(rf.peers) || N >= rf.lastIncludedIndex+1+len(rf.log) {
			//log[N]未同步到半数以上peer，过一会重试
			time.Sleep(50 * time.Millisecond)
		} else {
			//log[N]已经被半数节点同步了

			rf.mu.Lock()

			if rf.log[rf.getIndex(N)].Term == rf.currentTerm {
				rf.ok = true
			}
			//这个leader在当前任期成功处理过写请求，老任期的日志也可以apply
			if rf.ok {
				rf.commitIndex.Store(int32(N))
			}
			N++
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// 切换角色，调用方加锁
func (rf *Raft) switchRoleTo(role ROLE) {
	rf.DPrintln(BDebug, "变成了", role)
	rf.role = role
	switch role {
	case FOLLOWER:
		{
			rf.votedFor = -1
		}
	case CANDIDATE:
		{
			rf.currentLeader = -1
		}
	case LEADER:
		{
			rf.DPrintln(CDebug, rf.currentTerm, "成为了LEADER")
			rf.currentLeader = rf.me
			rf.ok = false
			for i := 0; i < len(rf.peers); i++ {
				rf.nextIndex[i] = rf.lastIncludedIndex + 1 + len(rf.log)
				rf.matchIndex[i] = rf.nextIndex[i] - 1
			}
			go rf.updateCommitIndex()

			// 成为LEADER后要立刻向集群发送心跳（刷新follower的选举计时器，防止其他人发起选举），不能等到ticker函数里面的选举休眠恢复
			go func() {
				for _, isleader := rf.GetState(); isleader && !rf.killed(); _, isleader = rf.GetState() {
					rf.sendAppend()
					time.Sleep(HeartBeatTimeoutMs * time.Millisecond)
				}
				rf.DPrintln(DDebug, "不是leader了")
			}()
		}
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// AppendEntries
type AppendEntriesArgs struct {
	Term              int         //leader’s term
	LeaderId          int         //so follower can redirect clients
	PrevLogIndex      int         //index of log entry immediately preceding new ones
	PrevLogTerm       int         //term of prevLogIndex entry
	Entries           []*LogEntry //log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommitIndex int         //leader’s commitIndex
}

type AppendEntriesReply struct {
	Term    int  //currentTerm, for leader to update itself
	Success bool //true if follower contained entry matching prevLogIndex and prevLogTerm
}

// 接受者的实现
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	rf.DPrintln(CDebug, "AppendEntries ", rf.currentTerm, args.LeaderId, args.Term, args.Term < rf.currentTerm)
	//1. Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	} else if args.Term > rf.currentTerm {
		//如果碰到了比自己任期大的节点，更新自己的信息
		rf.currentTerm = args.Term
		rf.switchRoleTo(FOLLOWER)
		reply.Term = args.Term
	}
	rf.currentLeader = args.LeaderId
	rf.appendEvent.Store(true)
	real_prevLogIndex := rf.getIndex(args.PrevLogIndex)
	//2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	if real_prevLogIndex < -1 {
		panic("real_prevLogIndex < -1")
	}
	if len(rf.log) <= real_prevLogIndex {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	rf.DPrintln(DDebug, "okk")
	// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
	if real_prevLogIndex != -1 && rf.log[real_prevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		rf.DPrintln(CDebug, "conflicts")
		rf.log = rf.log[:real_prevLogIndex]
		return
	}
	//now rf.log[args.PrevLogIndex].Term == args.PrevLogTerm

	//4. 如果 leader 复制的日志本地没有，则直接追加存储。

	rf.log = rf.log[:real_prevLogIndex+1]
	rf.log = append(rf.log, args.Entries...)
	reply.Success = true
	//5. 如果 leaderCommit>commitIndex，设置本地 commitIndex 为 leaderCommit 和最新日志索引中 较小的一个。
	if args.LeaderCommitIndex > int(rf.commitIndex.Load()) {
		rf.commitIndex.Store(int32(min(args.LeaderCommitIndex, rf.lastIncludedIndex+len(rf.log))))
	}
}

func (rf *Raft) SendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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

	term, isLeader := rf.GetState()
	if !isLeader || rf.killed() {
		return -1, -1, false
	}
	index := -1

	rf.mu.Lock()

	defer rf.mu.Unlock()
	defer rf.persist()

	index = rf.lastIncludedIndex + len(rf.log) + 1
	rf.DPrintln(DDebug, "Start: index:", index, "command: ", command)
	rf.log = append(rf.log, &LogEntry{
		Index:   index,
		Term:    rf.currentTerm,
		Command: command,
	})
	for idx, log := range rf.log {
		rf.DPrintln(BDebug, "Start ", idx, *log)
	}

	return index + 1, term, isLeader
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
	// Your code here, if desired.
	rf.mu.Lock()
	rf.persist()
	rf.mu.Unlock()

	atomic.StoreInt32(&rf.dead, 1)
}

type InstallSnapshotArgs struct {
	Term              int    // leader 的任期
	LeaderId          int    //参与者重定向到 leader
	LastIncludedIndex int    //快照替换的最后一条日志记录的索引
	LastIncludeTerm   int    //快照替换的最后一条日志记录的索引的任期 offset 快照中块的字节偏移量
	Data              []byte //数据
}
type InstallSnapshotReply struct {
	Term    int
	Success bool
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {

	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	} else if args.Term > rf.currentTerm {
		//如果碰到了比自己任期大的节点，更新自己的信息

		rf.mu.Lock()

		rf.currentTerm = args.Term
		rf.switchRoleTo(FOLLOWER)
		reply.Term = args.Term
		rf.mu.Unlock()
	}
	rf.DPrintln(DDebug, "InstallSnapshot: ", args.LastIncludedIndex, args.LastIncludeTerm)
	if args.Data == nil {
		rf.DPrintln(DDebug, args.LeaderId, "nil data")
	}

	rf.mu.Lock()
	rf.applyCh <- ApplyMsg{
		CommandValid: false,

		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludeTerm,
		SnapshotIndex: args.LastIncludedIndex + 1,
	}
	rf.snapshot = args.Data
	rf.log = []*LogEntry{}
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludeTerm
	rf.persist()
	rf.mu.Unlock()
	rf.lastApplied.Store(int32(args.LastIncludedIndex))
	rf.appendEvent.Store(true)

	reply.Success = true
}

// 作为leader，发送心跳&append，同步log给follower
func (rf *Raft) sendAppend() {
	rf.DPrintln(ADebug, rf.currentTerm, rf.role, "sendAppend")
	item, _ := rf.GetState()
	for index := range rf.peers {
		if index == rf.me {
			continue
		}
		go func(index int) {
			//LEADER向index发送AppendEntries
			args := &AppendEntriesArgs{
				Term:     item,
				LeaderId: rf.me,
				//对齐：prev不断向前缩，直到找到leader与FOLLOWER相同的最后一条日志，
				PrevLogIndex: rf.nextIndex[index] - 1, //TODO
				PrevLogTerm:  0,

				Entries:           nil,
				LeaderCommitIndex: int(rf.commitIndex.Load()),
			}
			realPrevLogIndex := rf.getIndex(args.PrevLogIndex) //prevlogindex在leader的log数组中真实下标
			rf.DPrintln(DDebug, index, "args.PrevLogIndex: ", args.PrevLogIndex, "realPrevLogIndex: ", realPrevLogIndex)
			if args.PrevLogIndex >= 0 && realPrevLogIndex < -1 { //已经被snapshot机制丢弃了，需要installsnapshot
				rf.mu.Lock()
				_args := &InstallSnapshotArgs{
					Term:              rf.currentTerm,
					LeaderId:          rf.me,
					LastIncludedIndex: rf.lastIncludedIndex,
					LastIncludeTerm:   rf.lastIncludedTerm,
					Data:              rf.snapshot,
				}
				rf.mu.Unlock()
				_reply := &InstallSnapshotReply{}
				if okk := rf.sendInstallSnapshot(index, _args, _reply); okk {
					rf.mu.Lock()
					if _reply.Term > rf.currentTerm {
						rf.currentTerm = _reply.Term
						rf.switchRoleTo(FOLLOWER)
					} else {
						//installsnapshot成功，更新rf.matchIndex[index]
						rf.matchIndex[index] = rf.lastIncludedIndex
						rf.nextIndex[index] = rf.matchIndex[index] + 1
					}
					rf.mu.Unlock()
				}
				return
			}

			if 0 <= realPrevLogIndex && realPrevLogIndex < len(rf.log) { //prev还在
				args.PrevLogTerm = rf.log[realPrevLogIndex].Term
			} else if realPrevLogIndex == -1 {
				args.PrevLogTerm = rf.lastIncludedTerm
			}
			if realPrevLogIndex+1 < len(rf.log) {
				args.Entries = rf.log[realPrevLogIndex+1:]
			}
			reply := &AppendEntriesReply{}
			if ok := rf.SendAppendEntries(index, args, reply); rf.role != LEADER || !ok {
				return
			}
			if !reply.Success && reply.Term > rf.currentTerm {
				//有follower的任期比我大
				rf.currentTerm = reply.Term

				rf.mu.Lock()

				rf.switchRoleTo(FOLLOWER)
				rf.mu.Unlock()
				return
			}

			rf.mu.Lock()

			if reply.Success {
				//append成功，更新rf.matchIndex[index]
				rf.matchIndex[index] = args.PrevLogIndex + len(args.Entries)
				rf.nextIndex[index] = rf.matchIndex[index] + 1
			} else {
				//append失败，需要减少nextIndex[peer]并重试
				//TODO nextIndex的回退逻辑
				rf.nextIndex[index] = max(0, rf.nextIndex[index]-5, rf.lastIncludedIndex+1)
			}
			rf.mu.Unlock()
		}(index)
	}
}

func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}

func (rf *Raft) ticker() {
	//You'll need to write code that takes actions periodically or after delays in time.
	// The easiest way to do this is to create a goroutine with a loop that calls time.Sleep();
	// see the ticker() goroutine that Make() creates for this purpose.
	// Don't use Go's time.Timer or time.Ticker, which are difficult to use correctly.

	time.Sleep(time.Duration(rand.Intn(VoteTimeoutMs)) * time.Millisecond)
	for !rf.killed() {
		// Your code here (3A)
		// Check if a leader election should be started.

		rf.mu.Lock()

		curr_role := rf.role
		rf.mu.Unlock()
		rf.DPrintln(ADebug, "ticker")
		switch curr_role {
		case FOLLOWER:
			{
				if rf.voteEvent.Load() {
					// 收到了投票事件，刷新选举计时器
					rf.DPrintln(ADebug, "收到投票事件")
					rf.voteEvent.Store(false)
					time.Sleep(time.Duration(VoteTimeoutMs+rand.Intn(VoteTimeoutMs)) * time.Millisecond)
				} else if rf.appendEvent.Load() {
					//收到了同步事件
					rf.DPrintln(ADebug, "收到同步事件")
					rf.appendEvent.Store(false)
					time.Sleep(time.Duration(VoteTimeoutMs+rand.Intn(VoteTimeoutMs)) * time.Millisecond)
				} else {
					//选举计时器超时，成为Candidate。立刻再进入一次循环，发起选举

					rf.mu.Lock()

					rf.switchRoleTo(CANDIDATE)
					rf.mu.Unlock()
				}
			}
		case CANDIDATE:
			{
				// 发起一轮选举
				go rf.startElection()
				time.Sleep(time.Duration(VoteTimeoutMs+rand.Intn(HeartBeatTimeoutMs)) * time.Millisecond)
			}
		case LEADER:
			{
				//心跳逻辑在switch函数中，因为成为leader之后就要立刻开始工作
				time.Sleep(time.Duration(HeartBeatTimeoutMs) * time.Millisecond)
			}
		}
	}
}

func (rf *Raft) startElection() {
	// 优化：先测试一下自己能否ping通半数节点
	count := atomic.Int32{}
	var okk atomic.Bool
	okk.Store(false)
	count.Store(1)
	for index := range rf.peers {
		go func(index int) {
			if ok := rf.sendRequestVote(index, &RequestVoteArgs{
				CandidateId: -1,
			}, &RequestVoteReply{}); ok {
				count.Add(1)
			}
		}(index)
	}
	time.Sleep(HeartBeatTimeoutMs * time.Millisecond)
	if int(count.Load())*2 < len(rf.peers) {
		rf.DPrintln(ADebug, "无法连通半数以上节点")
		rf.mu.Lock()
		rf.switchRoleTo(FOLLOWER)
		rf.mu.Unlock()
		return
	}

	//发起一轮选举
	//1.1 增加 currentTerm
	rf.mu.Lock()
	rf.currentTerm++
	//1.2 选举自己
	rf.votedFor = rf.me
	rf.DPrintln(DDebug, rf.currentTerm, "发起选举")
	rf.voteCount.Store(1)
	rf.persist()
	rf.mu.Unlock()
	//1.4 并行发送选举请求到其他所有机器
	for index := range rf.peers {
		if index == rf.me {
			continue
		}
		go func(index int) {
			args := &RequestVoteArgs{
				Term:        rf.currentTerm,
				CandidateId: rf.me,
			}
			if len(rf.log) != 0 {
				args.LastLogTerm = rf.log[len(rf.log)-1].Term
				args.LastLogIndex = rf.log[len(rf.log)-1].Index
			} else {
				args.LastLogTerm = rf.lastIncludedTerm
				args.LastLogIndex = rf.lastIncludedIndex
			}
			reply := &RequestVoteReply{
				Term:        0,
				VoteGranted: false,
			}
			ok := rf.sendRequestVote(index, args, reply)

			rf.mu.Lock()

			role := rf.role
			rf.mu.Unlock()
			if role != CANDIDATE || !ok {
				return
			}
			if reply.VoteGranted {
				rf.voteCount.Add(1)
				if rf.role == CANDIDATE && int(rf.voteCount.Load())*2 > len(rf.peers) {
					//拿到过半的选票，成为leader
					rf.mu.Lock()
					rf.switchRoleTo(LEADER)
					rf.mu.Unlock()
					return
				}
			} else if reply.Term > rf.currentTerm {
				//有follower的任期比我大，转化为follower
				rf.currentTerm = reply.Term
				rf.mu.Lock()
				rf.switchRoleTo(FOLLOWER)
				rf.mu.Unlock()
			}
		}(index)
	}
}

// 所有节点都要做的事情，向上层状态机apply log
func (rf *Raft) applyMsg() {
	for !rf.killed() {

		l := max(int(rf.lastApplied.Load()), rf.lastIncludedIndex)
		r := int(rf.commitIndex.Load())
		if l < r && len(rf.log) > 0 {
			// lastApplied初始化是-1
			//apply log[lastApplied+1, commitIndex]
			rf.DPrintln(DDebug, "APPLY: ", l+1, r)
			// rf.logger.Println("APPLY: ", l+1, r)
			for i := l + 1; i <= r; i++ {
				rf.mu.Lock()
				if rf.getIndex(i) < 0 {
					rf.logger.Println(i, rf.getIndex(i))
					continue
				}
				rf.DPrintln(DDebug, "apply: ", i, "real: ", rf.getIndex(i), rf.log[rf.getIndex(i)].Command)
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      rf.log[rf.getIndex(i)].Command,
					CommandIndex: i + 1,
				}
				rf.mu.Unlock()
				time.Sleep(10 * time.Millisecond)
			}
			rf.lastApplied.Store(int32(r))
		}
		time.Sleep(70 * time.Millisecond)
	}
}

func (rf *Raft) resume() {
	rf.readPersist(rf.persister.ReadRaftState())
	rf.snapshot = rf.persister.ReadSnapshot()
	if len(rf.snapshot) > 0 {
		go func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			// fmt.Printf("rf.lastIncludedIndex: %v\n", rf.lastIncludedIndex)
			rf.applyCh <- ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.snapshot,
				SnapshotIndex: rf.lastIncludedIndex + 1,
				SnapshotTerm:  rf.lastIncludedTerm,
			}
		}()
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
	rf.logger = *log.New(os.Stderr, "peer["+strconv.Itoa(me)+"] ", log.Lshortfile)
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.currentLeader = -1
	// Your initialization code here (3A, 3B, 3C).
	rf.applyCh = applyCh
	rf.role = FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.voteEvent = atomic.Bool{}
	rf.voteEvent.Store(false)
	rf.appendEvent = atomic.Bool{}
	rf.appendEvent.Store(false)
	rf.commitIndex.Store(-1)
	rf.lastApplied.Store(-1)
	//as leader
	rf.nextIndex = make([]int, 100)
	rf.matchIndex = make([]int, 100)
	//3D

	rf.lastIncludedIndex = -1

	// initialize from state persisted before a crash
	// rf.readPersist(persister.ReadRaftState())
	rf.resume()

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyMsg()
	return rf
}

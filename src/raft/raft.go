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
	"log"
	"math/rand"
	"sync"
	"time"
)
import "labrpc"

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//

//
// A Go object implementing a single Raft peer.
//\
const AppendEntriesInterval = time.Duration(100 * time.Millisecond) // sleep time between successive AppendEntries call
const ElectionTimeout = time.Duration(1000 * time.Millisecond)

// 生成1~2倍的随机等待时间长度
func newRandDuration(minDuration time.Duration) time.Duration {
	extra := time.Duration(rand.Int63()) % minDuration
	return time.Duration(minDuration + extra)
}

type Raft struct {
	mu                sync.Mutex          // Lock to protect shared access to this peer's state 单个节点内的同步锁
	peers             []*labrpc.ClientEnd // RPC end points of all peers
	persister         *Persister          // Object to hold this peer's persisted state
	me                int                 // this peer's index into peers[]
	leaderId          int                 // leader's id
	currentTerm       int                 // latest term server has seen, initialized to 0
	votedFor          int                 // candidate that received vote in current term
	commitIndex       int                 // index of highest log entry known to be committed, initialized to 0
	lastApplied       int                 // index of highest log entry applied to state machine, initialized to 0
	lastIncludedIndex int                 // index of the last entry in the log that snapshot replaces, initialized to 0
	logIndex          int                 // index of next log entry to be stored, initialized to 1
	state             serverState         // state of server
	shutdown          chan struct{}       // shutdown gracefully
	log               []LogEntry          // log entries
	nextIndex         []int               // for each server, index of the next log entry to send to that server
	matchIndex        []int               // for each server, index of highest log entry, used to track committed index
	applyCh           chan ApplyMsg       // apply to client
	notifyApplyCh     chan struct{}       // notify to apply
	electionTimer     *time.Timer         // electionTimer, kick off new leader election when time out
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock() // use synchronization to ensure visibility
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	data := rf.getPersistState()     //获取当前持久化状态
	rf.persister.SaveRaftState(data) //保存状态
}

func (rf *Raft) getPersistState() []byte {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.logIndex)
	e.Encode(rf.commitIndex)
	e.Encode(rf.lastApplied)
	e.Encode(rf.log)
	data := w.Bytes()
	return data
}

// 根据持久化保存的状态恢复自己的状态
func (rf *Raft) restorePersistState() {
	data := rf.persister.ReadRaftState()
	if data == nil || len(data) < 1 { // bootstrap without any state
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	//初始化数值
	currentTerm, votedFor, lastIncludedIndex, logIndex, commitIndex, lastApplied := 0, 0, 0, 0, 0, 0
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&logIndex) != nil ||
		d.Decode(&commitIndex) != nil ||
		d.Decode(&lastApplied) != nil ||
		d.Decode(&rf.log) != nil {
		//状态信息有缺失
		log.Fatal("Error in unmarshal raft state")
	}
	rf.currentTerm, rf.votedFor, rf.lastIncludedIndex, rf.logIndex, rf.commitIndex, rf.lastApplied = currentTerm, votedFor, lastIncludedIndex, logIndex, commitIndex, lastApplied
}

// RequestVote RPC handler.
// 接收到投票请求的消息处理
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Err, reply.Server = OK, rf.me
	if rf.currentTerm == args.Term && rf.votedFor == args.CandidateId { //不就是你刚跟我要了票吗
		reply.VoteGranted, reply.Term = true, rf.currentTerm //好好想想,我的票没丢吧
		return
	}
	if rf.currentTerm > args.Term || //竞选任期落后还想个啥,别想了
		(rf.currentTerm == args.Term && rf.votedFor != -1) { // 抱歉,这个任期内,我选了以为支持者,我很忠诚
		reply.Term, reply.VoteGranted = rf.currentTerm, false // 就是不投给你
		return
	}
	if args.Term > rf.currentTerm { //竞选者比我当前所处任期要新
		rf.currentTerm, rf.votedFor = args.Term, -1 //更新下我的所处任期,但是我还没决定把票给你
		if rf.state != Follower {                   // 比我竞选或当选的任期更新, 我放弃当前的身份成为一个跟随者
			rf.electionTimer.Stop()
			rf.electionTimer.Reset(newRandDuration(ElectionTimeout)) //等一段时间,大家要是这一任没给我个结果,我就重新参选
			rf.state = Follower
		}
	}
	rf.leaderId = -1                                    // 有人成功发起竞选, 当前无领导重置自己的领导者ID
	reply.Term = args.Term                              //当前选举的任期没毛病
	lastLogIndex := rf.logIndex - 1                     //这是我最新的日志编号
	lastLogTerm := rf.getLogEntry(lastLogIndex).LogTerm //这是我最新日志保存时候的任期
	if lastLogTerm > args.LastLogTerm ||                // 我最新的日志的存储任期比你新
		(lastLogTerm == args.LastLogTerm && lastLogIndex > args.LastLogIndex) { // 同领导者任期,我存储比你更新的日志信息
		reply.VoteGranted = false //我不能把票给你,你太落后了
		return
	}
	reply.VoteGranted = true       //日志编号没错,竞选任期也对,没毛病,你拿到了我的选票(有可能就是自己给自己)
	rf.votedFor = args.CandidateId //你就是这一届我认定的南波万
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(newRandDuration(ElectionTimeout)) //票都给你了,我就不着急参选了,等一会儿看看
	rf.persist()
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm > args.Term { // RPC call comes from an illegitimate leader
		reply.Term, reply.Success = rf.currentTerm, false
		return
	} // else args.Term >= rf.currentTerm

	reply.Term = args.Term
	rf.leaderId = args.LeaderId
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(newRandDuration(ElectionTimeout)) // reset electionTimer
	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = args.Term, -1
	}
	rf.state = Follower
	logIndex := rf.logIndex
	prevLogIndex := args.PrevLogIndex
	if prevLogIndex < rf.lastIncludedIndex {
		reply.Success, reply.ConflictIndex = false, rf.lastIncludedIndex+1
		return
	}
	if logIndex <= prevLogIndex || rf.getLogEntry(prevLogIndex).LogTerm != args.PrevLogTerm { // follower don't agree with leader on last log entry
		conflictIndex := Min(rf.logIndex-1, prevLogIndex)
		conflictTerm := rf.getLogEntry(conflictIndex).LogTerm
		floor := Max(rf.lastIncludedIndex, rf.commitIndex)
		for ; conflictIndex > floor && rf.getLogEntry(conflictIndex-1).LogTerm == conflictTerm; conflictIndex-- {
		}
		reply.Success, reply.ConflictIndex = false, conflictIndex
		return
	}
	reply.Success, reply.ConflictIndex = true, -1
	i := 0
	for ; i < args.Len; i++ {
		if prevLogIndex+1+i >= rf.logIndex {
			break
		}
		if rf.getLogEntry(prevLogIndex+1+i).LogTerm != args.Entries[i].LogTerm {
			rf.logIndex = prevLogIndex + 1 + i
			truncationEndIndex := rf.logIndex - rf.lastIncludedIndex
			rf.log = append(rf.log[:truncationEndIndex]) // delete any conflicting log entries
			break
		}
	}
	for ; i < args.Len; i++ {
		rf.log = append(rf.log, args.Entries[i])
		rf.logIndex += 1
	}
	oldCommitIndex := rf.commitIndex
	rf.commitIndex = Max(rf.commitIndex, Min(args.CommitIndex, args.PrevLogIndex+args.Len))
	rf.persist()
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(newRandDuration(ElectionTimeout)) // reset electionTimer
	if rf.commitIndex > oldCommitIndex {
		rf.notifyApplyCh <- struct{}{}
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//给我票
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, replyCh chan<- RequestVoteReply) {
	var reply RequestVoteReply
	if !rf.peers[server].Call("Raft.RequestVote", &args, &reply) {
		reply.Err, reply.Server = ErrRPCFail, server
	}
	replyCh <- reply
}

// Start
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
// leader处理日志请求
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return -1, -1, false
	}
	index := rf.logIndex
	entry := LogEntry{LogIndex: index, LogTerm: rf.currentTerm, Command: command}
	if offsetIndex := rf.logIndex - rf.lastIncludedIndex; offsetIndex < len(rf.log) {
		rf.log[offsetIndex] = entry
	} else {
		rf.log = append(rf.log, entry)
	}
	rf.matchIndex[rf.me] = rf.logIndex
	rf.logIndex += 1
	rf.persist()
	go rf.broadcast()
	return index, rf.currentTerm, true
}

// 发起竞选
func (rf *Raft) campaign() {
	rf.mu.Lock()
	if rf.state == Leader {
		rf.mu.Unlock()
		return
	}
	rf.leaderId = -1     // 当前无领导者
	rf.state = Candidate // 将自己转为候选者
	rf.currentTerm += 1  // 任期加一
	rf.votedFor = rf.me  // 给自己投票
	currentTerm, lastLogIndex, me := rf.currentTerm, rf.logIndex-1, rf.me
	logEntry := rf.getLogEntry(lastLogIndex)
	DPrintf("%d at %d start election, last index %d last term %d last entry %v",
		rf.me, rf.currentTerm, lastLogIndex, logEntry.LogTerm, rf.log[lastLogIndex])
	rf.persist() //记录自己的状态变更
	args := RequestVoteArgs{Term: currentTerm, CandidateId: rf.me, LastLogIndex: lastLogIndex, LastLogTerm: logEntry.LogTerm}
	electionDuration := newRandDuration(ElectionTimeout) //重置选举等待时间
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(electionDuration)
	timer := time.After(electionDuration) // in case there's no quorum, this election should timeout
	rf.mu.Unlock()
	replyCh := make(chan RequestVoteReply, len(rf.peers)-1) //保存返回信息的通道
	for i := 0; i < len(rf.peers); i++ {
		if i != me {
			go rf.sendRequestVote(i, args, replyCh) //向其他节点索要投票,第一轮
		}
	}
	voteCount, threshold := 0, len(rf.peers)/2 // 获得票数,当选票数阈值:当前的其他节点总数的一半
	for voteCount < threshold {                //不停向其他人要票,直到获选或其他人获选
		select {
		case <-rf.shutdown: //通信关闭 停止选举
			return
		case <-timer: // 当前选举时间结束,重新等待
			return
		case reply := <-replyCh: //获取投票结果, 索票索要
			if reply.Err != OK { //返回信息有误重新索要
				go rf.sendRequestVote(reply.Server, args, replyCh)
			} else if reply.VoteGranted {
				voteCount += 1
			} else { // since other server don't grant the vote, check if this server is obsolete
				rf.mu.Lock()
				if rf.currentTerm < reply.Term {
					rf.stepDown(reply.Term) //主动退位
				}
				rf.mu.Unlock()
			}
		}
	}
	// 拿到了过半的投票
	rf.mu.Lock()
	if rf.state == Candidate { // check if server is in candidate state before becoming a leader
		DPrintf("CANDIDATE: %d receive enough vote and becoming a new leader", rf.me)
		rf.state = Leader
		//获选后初始化数据
		peersNum := len(rf.peers)
		rf.nextIndex, rf.matchIndex = make([]int, peersNum), make([]int, peersNum)
		for i := 0; i < peersNum; i++ {
			rf.nextIndex[i] = rf.logIndex
			rf.matchIndex[i] = 0
		}
		go rf.heartbeat()       //发送心跳 也给自己发 这套系统的领导者的定时器是不关的
		go rf.notifyNewLeader() //通知产生了新的leader
	} // if server is not in candidate state, then another server may establishes itself as leader
	rf.mu.Unlock()
}

func (rf *Raft) notifyNewLeader() {
	rf.applyCh <- ApplyMsg{CommandValid: false, CommandIndex: -1, CommandTerm: -1, Command: "新大哥"}
}

// 心跳--定期发空包给所有的follower
func (rf *Raft) heartbeat() {
	timer := time.NewTimer(AppendEntriesInterval)
	for {
		select {
		case <-rf.shutdown:
			return
		case <-timer.C: //节点心跳时间到了，leader发送心跳
			if _, isLeader := rf.GetState(); !isLeader {
				return
			}
			go rf.broadcast() //leader广播log
			timer.Reset(AppendEntriesInterval)
		}
	}
}

//leader广播log信息,可能是心跳(空包),可能是(操作日志)
func (rf *Raft) broadcast() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for follower := 0; follower < len(rf.peers); follower++ {
		if follower != rf.me {
			go rf.sendLogEntry(follower) //单个发送操作
		}
	}
}

//获取指定节点的指定log
func (rf *Raft) getLogEntry(i int) LogEntry {
	offsetIndex := i - rf.lastIncludedIndex
	return rf.log[offsetIndex]
}

//获取指定范围的log
func (rf *Raft) getRangeLogEntry(fromInclusive, toExclusive int) []LogEntry {
	from := fromInclusive - rf.lastIncludedIndex
	to := toExclusive - rf.lastIncludedIndex
	return append([]LogEntry{}, rf.log[from:to]...)
}

//leader单个发送log给follower
func (rf *Raft) sendLogEntry(follower int) {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	if rf.nextIndex[follower] <= rf.lastIncludedIndex {
		//当前follower的日志未同步,leader发送消息同步日志
		go rf.sendLogSnapshot(follower)
		rf.mu.Unlock()
		return
	}
	prevLogIndex := rf.nextIndex[follower] - 1
	prevLogTerm := rf.getLogEntry(prevLogIndex).LogTerm
	args := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm, CommitIndex: rf.commitIndex, Len: 0, Entries: nil}
	if rf.nextIndex[follower] < rf.logIndex {
		entries := rf.getRangeLogEntry(prevLogIndex+1, rf.logIndex)
		args.Entries, args.Len = entries, len(entries)
	}
	rf.mu.Unlock()
	//接受者回复日志消息
	var reply AppendEntriesReply
	if rf.peers[follower].Call("Raft.AppendEntries", &args, &reply) {
		rf.mu.Lock()
		if !reply.Success {
			if reply.Term > rf.currentTerm { // the leader is obsolete
				rf.stepDown(reply.Term)
			} else { // follower 任期小于等于 leader
				rf.nextIndex[follower] = Max(1, Min(reply.ConflictIndex, rf.logIndex))
				if rf.nextIndex[follower] <= rf.lastIncludedIndex {
					go rf.sendLogSnapshot(follower)
				}
			}
		} else {
			// reply.Success is true
			prevLogIndex, logEntriesLen := args.PrevLogIndex, args.Len
			if prevLogIndex+logEntriesLen >= rf.nextIndex[follower] { // in case apply arrive in out of order
				rf.nextIndex[follower] = prevLogIndex + logEntriesLen + 1
				rf.matchIndex[follower] = prevLogIndex + logEntriesLen
			}
			toCommitIndex := prevLogIndex + logEntriesLen
			if rf.canCommit(toCommitIndex) { //提交当前的日志,将其持久化存储,清空消息通道
				rf.commitIndex = toCommitIndex
				rf.persist() //持久化存储
				rf.notifyApplyCh <- struct{}{}
			}
		}
		rf.mu.Unlock()
	}
}

// leader退位
func (rf *Raft) stepDown(term int) {
	rf.currentTerm = term
	rf.state = Follower
	rf.votedFor, rf.leaderId = -1, -1
	rf.persist() //保存当前的状态变化
	//重置选举时间
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(newRandDuration(ElectionTimeout))
}

// leader发送当前存储日志的快照 follower
func (rf *Raft) sendLogSnapshot(follower int) {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	args := InstallSnapshotArgs{Term: rf.currentTerm, LeaderId: rf.me, LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm: rf.getLogEntry(rf.lastIncludedIndex).LogTerm, Data: rf.persister.ReadSnapshot()}
	rf.mu.Unlock()
	var reply InstallSnapshotReply
	if rf.peers[follower].Call("Raft.InstallSnapshot", &args, &reply) {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.stepDown(reply.Term)
		} else {
			rf.nextIndex[follower] = Max(rf.nextIndex[follower], rf.lastIncludedIndex+1)
			rf.matchIndex[follower] = Max(rf.matchIndex[follower], rf.lastIncludedIndex)
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) canCommit(index int) bool {
	if index < rf.logIndex && rf.commitIndex < index && rf.getLogEntry(index).LogTerm == rf.currentTerm {
		//当前同一任期, 申请存储的日志编号符合条件,比下一个要存储的小,比要提交的大
		majority, count := len(rf.peers)/2+1, 0
		for j := 0; j < len(rf.peers); j++ {
			if rf.matchIndex[j] >= index { //比已提交的日志标号要大或相等
				count += 1 //同意当前提交的申请
			}
		}
		return count >= majority
	} else {
		return false
	}
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	//啊 不能说话,不能回别人消息了,啥也干不了了
	rf.mu.Lock()
	defer rf.mu.Unlock()
	close(rf.shutdown)
}

//回复日志消息
func (rf *Raft) apply() {
	for {
		select {
		case <-rf.notifyApplyCh: //诶,收到一条日志消息,让我来康康,这玩意儿一半只有领导才给我发吧
			rf.mu.Lock()
			var commandValid bool
			var entries []LogEntry
			if rf.lastApplied < rf.lastIncludedIndex { //我记得我保存的,竟然不是我最新已经保存的,不行,我得重新保存一下
				commandValid = false
				rf.lastApplied = rf.lastIncludedIndex
				entries = []LogEntry{{LogIndex: rf.lastIncludedIndex, LogTerm: rf.log[0].LogTerm, Command: "InstallSnapshot"}}
			} else if rf.lastApplied < rf.logIndex && rf.lastApplied < rf.commitIndex { //马上要保存的和要提交的比我记得存好的都要新,可以保存一下
				commandValid = true
				entries = rf.getRangeLogEntry(rf.lastApplied+1, rf.commitIndex+1) //把这段要存起来的日志都拿出来
				rf.lastApplied = rf.commitIndex                                   //存下这些日志了,我得计一下
			}
			rf.persist() //记录一下我的当前状态
			rf.mu.Unlock()
			for _, entry := range entries { //这些日志都得处理一下
				rf.applyCh <- ApplyMsg{CommandValid: commandValid, CommandIndex: entry.LogIndex, CommandTerm: entry.LogTerm, Command: entry.Command}
			}
		case <-rf.shutdown:
			return
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

	// Your initialization code here.
	// 代表无领导人。
	rf.leaderId = -1
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.lastIncludedIndex = 0
	rf.logIndex = 1
	rf.log = []LogEntry{{0, 0, nil}} // log entry at index 0 is unused
	rf.state = Follower              // initializing as follower
	rf.shutdown = make(chan struct{})
	rf.applyCh = applyCh
	rf.notifyApplyCh = make(chan struct{}, 100)
	rf.electionTimer = time.NewTimer(newRandDuration(ElectionTimeout))
	rf.restorePersistState() // 看看我以前干啥了, 把业绩找回来
	go rf.apply()            //都让开 我开始回消息了
	go func() {
		for {
			select {
			case <-rf.electionTimer.C:
				//没人管我吗,那我自己要当领导
				rf.campaign()
			case <-rf.shutdown:
				//谁把我关了
				return
			}
		}
	}()
	return rf
}

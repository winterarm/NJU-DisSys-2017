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
const HeartbeatInterval = 100 * time.Millisecond // sleep time between successive AppendEntries call
const ElectionTimeout = 1000 * time.Millisecond

// 生成1~2倍的随机等待时间长度
func newRandDuration(minDuration time.Duration) time.Duration {
	extra := time.Duration(rand.Int63()) % minDuration
	return minDuration + extra
}

type Raft struct {
	mu                sync.Mutex          // Lock to protect shared access to this peer's state 单个节点内的同步锁
	peers             []*labrpc.ClientEnd // 其他节点RPC通信连接终端信息
	persister         *Persister          // 节点状态持久化机器
	me                int                 // 自己的编号
	leaderId          int                 // 当前任期认同的Leader
	currentTerm       int                 // 节点所处的任期 初始化值为0
	votedFor          int                 // 当前任期票型
	commitIndex       int                 // 已经提交的日志索引 初始化值为0
	lastApplied       int                 // 已经持久化的日志索引值 初始化值为0
	lastIncludedIndex int                 // 最后写入(可能未提交)的日志索引值 初始化值为0
	logIndex          int                 // 下一个写入的日志索引值, 初始化值为1
	state             serverState         // 节点的状态值
	shutdown          chan struct{}       // 关闭节点的通知通道
	log               []LogEntry          // 节点保存的日志
	nextIndex         []int               // 发送给节点的下一个日志索引值, Leader获选时初始化, 每个节点自己维护
	matchIndex        []int               // 与Leader的匹配的日志索引值, Leader获选时初始化, 每个节点自己维护
	applyCh           chan ApplyMsg       // apply to client 日志传输通道,用于持久化
	notifyApplyCh     chan struct{}       // notify to apply 通知日志变动
	electionTimer     *time.Timer         // 选举计时器
}

// GetState 当前所处任期, 是否自认为领导
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
func (rf *Raft) restorePersistState(data []byte) {
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
		(rf.currentTerm == args.Term && rf.votedFor != -1) { // 抱歉,这个任期内,我支持另一个人,我很忠诚
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
	rf.leaderId = -1                            // 有人成功发起竞选, 当前无领导重置自己的领导者ID
	reply.Term = args.Term                      //当前选举的任期没毛病
	lastLogIndex := rf.lastIncludedIndex        //这是我最新的日志编号
	lastLogTerm := rf.log[lastLogIndex].LogTerm //这是我最新日志保存时候的任期
	if lastLogTerm > args.LastLogTerm ||        // 我最新的日志的存储任期比你新
		(lastLogTerm == args.LastLogTerm && lastLogIndex > args.LastLogIndex) { // 同领导者任期,我存储比你更新的日志信息
		reply.VoteGranted = false //我不能把票给你,你日志不是最新的
		return
	}
	reply.VoteGranted = true       //日志编号没错,竞选任期也对,没毛病,你拿到了我的选票(有可能就是自己给自己)
	rf.votedFor = args.CandidateId //你就是这一届我认定的南波万
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(newRandDuration(ElectionTimeout)) //票都给你了,我就不着急参选了,等一会儿看看
	rf.persist()
}

// AppendEntries RPC handler 添加日志的处理
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm > args.Term { // RPC call comes from an illegitimate leader
		//我不接受可能来自老领导的消息
		DPrintf("APPEND_ERRT:Leader %d, Follower %d", args.LeaderId, rf.me)
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}
	reply.Term = args.Term
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(newRandDuration(ElectionTimeout)) // reset electionTimer
	if args.Term > rf.currentTerm {
		//来新领导了,但我不确定他就一定是,先把领导任期记一下,投票状态初始化
		rf.currentTerm, rf.votedFor = args.Term, -1
	}
	rf.state = Follower //面对新领导或者同任期的其他领导 我现在是领导的话就先退位, 成为跟随者
	rf.leaderId = args.LeaderId
	lastIncludedIndex := rf.lastIncludedIndex // 这是我已经记录下来的日志编号
	prevLogIndex := args.PrevLogIndex         // 新领导 认为我记到的地方,让我从这个日志往后开始记新的
	if lastIncludedIndex < prevLogIndex {
		//跟新领导说 我记得比较少 可能得重置一下我的日志了
		for ; lastIncludedIndex > rf.commitIndex && rf.log[lastIncludedIndex].LogTerm != args.PrevLogTerm; lastIncludedIndex-- {
		}
		reply.Success, reply.ConflictIndex = false, lastIncludedIndex+1
		return
	}
	DPrintf("CFLC_CHECK:Leader %d, Follower %d, LastIndex %d, PreIdx %d, Logs %v", args.LeaderId, rf.me, lastIncludedIndex, prevLogIndex, rf.log)
	if lastIncludedIndex >= prevLogIndex && rf.log[prevLogIndex].LogTerm != args.PrevLogTerm {
		// follower don't agree with leader on last log entry
		// 领导你记得不对呀, 我接下来记得编号比你以为我记得还小 我的日志存在问题 我可能缺日志了
		// 你想记得编号的日志 我在别的任期的领导那儿记过了
		conflictIndex := Min(lastIncludedIndex, prevLogIndex) //看看我们是从哪儿开始不一致的
		conflictTerm := rf.log[conflictIndex].LogTerm
		floor := rf.commitIndex
		for ; conflictIndex > floor && rf.log[conflictIndex].LogTerm == conflictTerm; conflictIndex-- {
		} // 看看咱到底是从哪儿开始不对的
		reply.Success, reply.ConflictIndex = false, conflictIndex
		rf.persist() //记录一下我的当前状态
		DPrintf("CFLC_GET:Leader %d, Follower %d, CnfxIdx %d, PreIdx %d", args.LeaderId, rf.me, conflictIndex, prevLogIndex)
		return
	}
	reply.Success, reply.ConflictIndex = true, -1
	i := 0
	for ; i < args.Len; i++ {
		if prevLogIndex+1+i >= rf.logIndex { //全是新的日志
			break
		}
		if rf.log[prevLogIndex+1+i].LogTerm != args.Entries[i].LogTerm {
			//当前我记的一些日志不是你想让我记的,我需要听你的
			rf.logIndex = prevLogIndex + 1 + i
			rf.lastIncludedIndex = prevLogIndex + i
			//truncationEndIndex := rf.logIndex - rf.lastIncludedIndex //丢弃掉这部分日志
			rf.log = append([]LogEntry{}, rf.log[:prevLogIndex+1+i]...)
			DPrintf("DEL_LOG:Leader %d, Follower %d, DelStart %d", args.LeaderId, rf.me, prevLogIndex)
			break
		}
	}
	for ; i < args.Len; i++ { //把新的日志保存下来
		rf.log = append(rf.log, args.Entries[i])
		DPrintf("APPEND_ONE:Leader %d, Follower %d, Idx %d, Log %v", args.LeaderId, rf.me, rf.logIndex, args.Entries[i])
		rf.logIndex += 1
		rf.lastIncludedIndex += 1
	}
	oldCommitIndex := rf.commitIndex
	rf.commitIndex = Max(rf.commitIndex, Min(args.CommitIndex, args.PrevLogIndex+args.Len)) //确定要提交的日志索引,并更新自己的提交索引记录
	rf.persist()                                                                            //持久化
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(newRandDuration(ElectionTimeout)) // 重置选举及时器
	if rf.commitIndex > oldCommitIndex {                     //当前有未持久化日志了
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
// leader处理客户端日志请求
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return -1, -1, false
	}
	index := rf.logIndex
	entry := LogEntry{LogIndex: index, LogTerm: rf.currentTerm, Command: command}
	rf.log = append(rf.log, entry)
	rf.matchIndex[rf.me] = rf.logIndex
	DPrintf("CMD_RECV:Leader %d, LogIndex %d, CMD %v", rf.me, rf.logIndex, command)
	rf.logIndex += 1
	rf.lastIncludedIndex += 1
	rf.persist()
	go rf.broadcast()
	return index, rf.currentTerm, true
}

// 发起竞选
func (rf *Raft) campaign() {
	rf.mu.Lock()
	rf.leaderId = -1     // 当前无领导者
	rf.state = Candidate // 将自己转为候选者
	rf.currentTerm += 1  // 任期加一
	rf.votedFor = rf.me  // 给自己投票
	currentTerm, lastIncludedIndex, me := rf.currentTerm, rf.lastIncludedIndex, rf.me
	logEntry := rf.log[lastIncludedIndex]
	DPrintf("START_ELEC:Server %d, Term %d , LastLog [%d, %d, %v]",
		rf.me, rf.currentTerm, lastIncludedIndex, logEntry.LogTerm, rf.log[lastIncludedIndex])
	rf.persist() //记录自己的状态变更
	args := RequestVoteArgs{Term: currentTerm, CandidateId: rf.me, LastLogIndex: lastIncludedIndex, LastLogTerm: logEntry.LogTerm}
	electionDuration := newRandDuration(ElectionTimeout) //重置选举等待时间
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(electionDuration)
	timer := time.After(electionDuration) // 选举定时器结束了
	rf.mu.Unlock()
	replyCh := make(chan RequestVoteReply, len(rf.peers)-1) //保存返回信息的通道
	for i := 0; i < len(rf.peers); i++ {
		if i != me {
			go rf.sendRequestVote(i, args, replyCh) //向其他节点索要投票,第一轮
		}
	}
	voteCount, threshold := 0, len(rf.peers)/2 // 获得票数,当选票数阈值:当前的其他节点总数的一半
	for voteCount < threshold {                //不停向其他人要票,直到选举期限内获选或选择退选
		select {
		case <-rf.shutdown: //通信关闭 关机了 停止选举
			return
		case <-timer: // 当前选举时间结束,重新等待,是一个新的timer,避免结束后,为什么不能用rf.electionTimer
			DPrintf("QUIT_CAMPAIGN:Candidate %d, Term%d", rf.me, rf.currentTerm)
			return
		case reply := <-replyCh: //获取投票结果
			if reply.Err != OK { //返回信息有误重新索要投票
				go rf.sendRequestVote(reply.Server, args, replyCh)
			} else if reply.VoteGranted {
				//索票成功回复, 每个节点投票成功信息只会处理一次
				DPrintf("GETVOTE:CANDIDATE %d, From %d", rf.me, reply.Server)
				voteCount += 1
			} else { //收到回复,但是告诉我不给我票,要么投给其他人,要么就是我的竞选任期过时了
				rf.mu.Lock()
				if rf.currentTerm < reply.Term {
					DPrintf("QUIT_CAMPAIGN, Leader %d,Term %d Out", rf.me, rf.currentTerm)
					rf.stepDown(reply.Term) //选举期过了, 主动退选
				}
				rf.mu.Unlock()
			}
		}
	}
	// 拿到了过半的投票
	rf.mu.Lock()
	if rf.state == Candidate { // check if server is in candidate state before becoming a leader
		DPrintf("WIN:CANDIDATE: %d", rf.me)
		rf.state = Leader
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = rf.logIndex
			rf.matchIndex[i] = 0
		}
		rf.electionTimer.Stop()
		go rf.heartbeat() //发送心跳
	}
	rf.mu.Unlock()
}

// 心跳--定期发空包给所有的follower
func (rf *Raft) heartbeat() {
	timer := time.NewTimer(HeartbeatInterval)
	for {
		select {
		case <-rf.shutdown:
			return
		case <-timer.C: //节点心跳时间到了，leader发送心跳
			if _, isLeader := rf.GetState(); !isLeader {
				return
			}
			go rf.broadcast() //leader广播log
			timer.Reset(HeartbeatInterval)
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

//leader单个发送log给follower
func (rf *Raft) sendLogEntry(follower int) {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	prevLogIndex := rf.nextIndex[follower] - 1
	prevLogTerm := rf.log[prevLogIndex].LogTerm
	preCommitIndex := rf.commitIndex
	for ; preCommitIndex > 0 && rf.log[preCommitIndex].LogTerm != prevLogTerm; preCommitIndex-- {
	}
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		CommitIndex:  preCommitIndex,
		Len:          0,
		Entries:      nil,
	}
	if prevLogIndex < rf.lastIncludedIndex {
		//计算需要同步的日志
		entries := rf.log[prevLogIndex+1 : rf.logIndex]
		args.Entries, args.Len = entries, len(entries)
		DPrintf("APPEND_CHECK:Leader %d, Follower %d, PreLogIdx %d, LOG_LEN %d", rf.me, follower, prevLogIndex, args.Len)
	}
	rf.mu.Unlock()
	var reply AppendEntriesReply
	if rf.peers[follower].Call("Raft.AppendEntries", &args, &reply) {
		//收到有效回复的处理
		rf.mu.Lock()
		if !reply.Success {
			if reply.Term > rf.currentTerm {
				rf.stepDown(reply.Term) // 我任期已经过了,有新的leader产生了
			} else {
				// 当前这个跟随者想要和我同步消息
				rf.nextIndex[follower] = Max(1, reply.ConflictIndex) //更新一下这个跟随者当前日志需要从哪儿开始同步
				if rf.nextIndex[follower] <= rf.lastIncludedIndex {
					//存在需要同步的日志, 发送我的日志快照给他,异步处理,不影响自己处理其他事情
					DPrintf("SYNC_ERR:Leader %d, Follower %d, startIndex %d", follower, rf.me, rf.nextIndex[follower])
					go rf.sendLogEntry(follower)
				}
			}
		} else {
			// 收到日志添加成功的回复
			if args.Len > 0 {
				DPrintf("APPEND_SUC:Leader %d, To %d, toCommitIndex %d, nextIndex %d", rf.me, follower, rf.lastIncludedIndex, rf.nextIndex[follower])
				// 日志添加成功 更新信息
				if prevLogIndex < rf.lastIncludedIndex {
					rf.nextIndex[follower] = rf.logIndex
					rf.matchIndex[follower] = rf.lastIncludedIndex
				}
				if rf.canCommit(rf.lastIncludedIndex) { //同意当前提交的日志索引值的节点数量过半,则提交当前的日志,将其持久化存储
					rf.commitIndex = rf.lastIncludedIndex
					rf.persist() //持久化存储状态,有日志提交
					//提交日志 日志本地化处理
					rf.notifyApplyCh <- struct{}{}
					DPrintf("COMMIT_LOG:Leader %d, index %d", rf.me, rf.lastIncludedIndex)
				}
			}
		}
		rf.mu.Unlock()
	}
}

// leader退位
func (rf *Raft) stepDown(term int) {
	DPrintf("STEPDOWM:Server %d, Term %d", rf.me, term)
	rf.currentTerm = term
	rf.state = Follower
	rf.votedFor, rf.leaderId = -1, -1
	rf.persist() //保存当前的状态变化
	//重置选举时间
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(newRandDuration(ElectionTimeout))
}

func (rf *Raft) canCommit(index int) bool {
	if index < rf.logIndex && rf.commitIndex < index && rf.log[index].LogTerm == rf.currentTerm {
		//申请提交的日志是当前任期申请的, 且日志编号符合以下条件,比下一个要存储的小,比要提交的大
		majority, count := len(rf.peers)/2+1, 0
		for j := 0; j < len(rf.peers); j++ {
			if rf.matchIndex[j] >= index { //比已匹配的日志标号要大或相等
				count += 1 //同意当前提交的申请
			}
		}
		return count >= majority //超过半数的节点已经复制了日志,可以提交
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

//我的日志有变动, 日志本地化存储
func (rf *Raft) apply() {
	for {
		select {
		case <-rf.notifyApplyCh: //有日志需要本地化存储
			rf.mu.Lock()
			var commandValid bool
			var entries []LogEntry
			if rf.lastApplied < rf.commitIndex {
				//要提交的比我已经持久化的都要新,可以做一下持久化处理
				commandValid = true
				entries = rf.log[rf.lastApplied+1 : rf.commitIndex+1] //把这段要持久化的日志都拿出来
				DPrintf("PERSIST_CHECK: Server %d, LastApplied %d, PersistNum %d", rf.me, rf.lastApplied, len(entries))
				rf.lastApplied = rf.commitIndex //持久化的日志索引值更新一下
			}
			rf.persist() //记录一下我的当前状态
			rf.mu.Unlock()
			for _, entry := range entries { //这些日志都得本地化处理
				DPrintf("PERSIST_LOG: Server %d, LogIndex %d", rf.me, entry.LogIndex)
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
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.leaderId = -1
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.lastIncludedIndex = 0
	rf.logIndex = 1
	rf.log = []LogEntry{{0, 0, 0}} // 0号空日志, 为无效日志
	rf.state = Follower            // 初始化身份为跟随者
	peersNum := len(rf.peers)
	rf.nextIndex, rf.matchIndex = make([]int, peersNum), make([]int, peersNum)
	rf.shutdown = make(chan struct{}) //初始化关机消息通知
	rf.applyCh = applyCh
	rf.notifyApplyCh = make(chan struct{}, 100)
	rf.electionTimer = time.NewTimer(newRandDuration(ElectionTimeout))
	rf.restorePersistState(persister.ReadRaftState()) // 看看我以前干啥了, 把业绩找回来
	go rf.apply()                                     // 检测日志消息变化,实现日志的更新
	go func() {
		for {
			select {
			case <-rf.electionTimer.C:
				go rf.campaign() //没人管我吗,那我自己要当领导
			case <-rf.shutdown:
				return //
			}
		}
	}()
	return rf
}

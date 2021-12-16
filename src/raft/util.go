package raft

import "log"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf("DEBUG----"+format, a...)
	}
	return
}

func Min(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func Max(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

type Err string

const (
	OK         = "OK"
	ErrRPCFail = "ErrRPCFail"
)

type serverState int32

const (
	Leader serverState = iota
	Follower
	Candidate
)

func (state serverState) String() string {
	switch state {
	case Leader:
		return "Leader"
	case Follower:
		return "Follower"
	default:
		return "Candidate"
	}
}

// ApplyMsg 持久化的数据结构
type ApplyMsg struct {
	CommandValid bool
	CommandIndex int
	CommandTerm  int
	Command      interface{}
}

// LogEntry 日志结构体定义
type LogEntry struct {
	LogIndex int
	LogTerm  int
	Command  interface{}
}

// RequestVoteArgs 投票请求参数结构体定义
type RequestVoteArgs struct {
	Term, // 参选的任期
	CandidateId, // 参选人ID
	LastLogIndex, // 参选人的最后接收的log的索引编号
	LastLogTerm int // 最后接收的log的任期Term
}

// RequestVoteReply 投票消息回复结构体定义
type RequestVoteReply struct {
	Err         Err
	Server      int
	VoteGranted bool // true 表示获得投票
	Term        int  // 告知参选者自己所处的任期Term
}

type AppendEntriesArgs struct {
	Term,
	LeaderId,
	PrevLogIndex, //同步开始的日志编号
	PrevLogTerm, //同步开始的日志Term
	CommitIndex int //可提交的日志编号
	Len     int        // 发送的日志数量
	Entries []LogEntry // 日志数据
}

type AppendEntriesReply struct {
	Success       bool // true 跟随者接收的最后的日志信息与请求添加者的信息一致
	Term          int  // 返回请求者和自己Term信息中更大的那个
	ConflictIndex int  // 若存在不一致日志,告知最早不一致的日志编号
}

// InstallSnapshotArgs 批量同步日志数据结构体
type InstallSnapshotArgs struct {
	Term             int
	LeaderId         int
	StartIndex       int
	LastIncludedTerm int
	Entries          []LogEntry // 同步的日志
}

// InstallSnapshotReply 批量同步日志的返回
type InstallSnapshotReply struct {
	Err  Err
	Term int //返回日志发送方和接收方Term更大的值
}

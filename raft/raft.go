/*
 * @Author: WXB 1763567512@qq.com
 * @Date: 2024-04-25 14:34:15
 * @LastEditors: WXB 1763567512@qq.com
 * @LastEditTime: 2024-05-20 16:29:36
 * @FilePath: /6.824/raft/raft.go
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
package raft

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

const LEADER = 1
const CANDIDATE = 2
const FOLLOWER = 3

const BROADCASTTIME = 100 // 超时时间
const ELECTIONTIMEOUTBASE = 1000
const ELECTIONTIMEOUTRANGE = 1000


type ApplyMsg struct {
	CommandValid bool
	Command interface{}
	CommandIndex int

	SnapshotValid bool
	Snapshot []byte
	SnapshotTerm int
	SnapshotIndex int
}


type LogEntry struct {
	Command interface{} //命令
	Term int // 任期
	Index int // 索引
}

type Raft struct {
	mu sync.Mutex
	peers []*labrpc.ClientEnd
	persister *Persister
	me int // 在peers中的索引
	dead int32
	applych chan ApplyMsg
	cond *sync.Cond
	quicklyCheck int32
	name string // 节点名字

	BroadcastTime int // 
	ElectionTimeout int // 选举超时器
	State int
	CurrentTerm int
	VotedFor int // 给谁投过票
	Log []LogEntry
	CommitIndex int // 已经提交的索引
	LastApplied int // 已经应用的索引
	NextIndex []int // 每一个peer的下一个索引
	MatchIndex []int // 目前匹配的日志索引

	LastIncludedIndex int // 快照的最后一条索引
	LastIncludedTerm int // 快照的最后一个任期号
}

func (rf *Raft) GetState() (int, bool) {
	if rf.killed() {
		return -1, false
	}

	var term int
	var isleader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.CurrentTerm
	isleader = rf.State == LEADER

	return term, isleader
}

func (rf *Raft) persist() {
	writer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(writer)

	// 序列化关键信息
	encoder.Encode(rf.CurrentTerm)
	encoder.Encode(rf.VotedFor)
	encoder.Encode(rf.Log)
	encoder.Encode(rf.LastIncludedIndex)
	encoder.Encode(rf.LastIncludedTerm)

	data := writer.Bytes()
	rf.persister.SaveRaftState(data)
	fmt.Printf("[%d] call Persist(),rf.CurrentTerm = %d,rf.VotedFor = %d,rf.Log len = %d,lastIncludedIndex = %d", rf.me, rf.CurrentTerm, rf.VotedFor, len(rf.Log), rf.LastIncludedIndex)
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reader := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(reader)

	var currentTerm int
	var votedFor int
	var log []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int

	if decoder.Decode(&currentTerm) != nil ||
		decoder.Decode(&votedFor) != nil ||
		decoder.Decode(&log) != nil ||
		decoder.Decode(&lastIncludedIndex) != nil ||
		decoder.Decode(&lastIncludedTerm) != nil {
		panic("readPersist : decode err")
	} else {
		rf.CurrentTerm = currentTerm
		rf.VotedFor = votedFor
		rf.Log = log
		rf.LastIncludedIndex = lastIncludedIndex
		rf.LastIncludedTerm = lastIncludedTerm
		fmt.Printf("[%d] call readPersist(),rf.CurrentTerm = %d,rf.VotedFor = %d,rf.Log len = %d,lastIncludedIndex = %d", rf.me, rf.CurrentTerm, rf.VotedFor, len(rf.Log), lastIncludedIndex)
	}
}

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludeedIndex int, snapshot []byte) bool  {
	return true
}

// 创建快照(index 之前)
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	if index <= rf.LastIncludedIndex {
		rf.mu.Unlock()
		return
	}
	fmt.Printf("[%d] CALL Snapshot index = %d", rf.me, index)
	rf.LastIncludedTerm = rf.GetLogIndex(index).Term // 设置快照索引

	// 压缩日志
	var log[]LogEntry
	for i := index + 1; i <= rf.GetLastEntry().Index; i++ {
		log = append(log, rf.GetLogIndex(i))
	}
	rf.Log = log
	rf.LastIncludedIndex = index

	writer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(writer)
	encoder.Encode(rf.CurrentTerm)
	encoder.Encode(rf.VotedFor)
	encoder.Encode(rf.Log)
	encoder.Encode(rf.LastIncludedIndex)
	encoder.Encode(rf.LastIncludedTerm)
	data := writer.Bytes()
	rf.persister.SaveStateAndSnapshot(data, snapshot)
	rf.mu.Unlock()
}

// 投票RPC参数
type RequestVoteArgs struct {
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

// 投票结果
type RequestVoteReply struct {
	Term int
	VoteGranted bool
}

// 日志复制参数
type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommit int
}


// 日志复制返回值
type AppendEntriesReply struct {
	Term int
	Success bool
	XTerm int
	XIndex int
	XLen int
}


// 快照请求参数
type InstallSnapshotArgs struct {
	Term int
	LeaderId int
	LastIncludedIndex int
	LastIncludedTerm int
	Snapshot []byte
}

type InstallSnapshotReply struct {
	Term int
}

//投票请求RPC
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.CurrentTerm
	LastEntry := rf.GetLastEntry()
	LastIndex := LastEntry.Index
	LastTerm := LastEntry.Term

	if rf.CurrentTerm > args.Term { // 当前任期大于对方
		reply.VoteGranted = false
		return
	} else if rf.CurrentTerm < args.Term { // 判断任期是不是比我大
		rf.State = FOLLOWER
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		rf.persist()
	}

	// 判断日志是不是最新
	if rf.VotedFor == -1 && (LastTerm < args.LastLogTerm || (LastTerm == args.LastLogTerm && LastIndex <= args.LastLogIndex)) {
		reply.VoteGranted = true
		rf.VotedFor = args.CandidateId
		rf.persist()
		rf.ElectionTimeout = GetElectionTimeout()
		fmt.Printf("[%d %d] VoteFor %d(term : %d)\n", rf.me, rf.State, args.CandidateId, rf.CurrentTerm)
	}
}

// 日志复制RPC
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Printf("[me: %d, state: %d] get AppendEntries from %d, LastIncludeIndex = %d,currterm = %d(PrevLogIndex:%d,PrevLogTerm:%d,Leaderterm:%d)\n",
							rf.me, rf.State, args.LeaderId, rf.LastIncludedIndex, rf.CurrentTerm, args.PrevLogIndex, args.PrevLogTerm, args.Term)

	reply.Term = rf.CurrentTerm
	reply.Success = true
	rf.ElectionTimeout = GetElectionTimeout()
	if args.Term < rf.CurrentTerm || rf.LastIncludedIndex > args.PrevLogIndex {
		reply.Success = false
		return
	}

	if rf.CurrentTerm < args.Term || rf.State == CANDIDATE {
		rf.State = FOLLOWER
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		rf.cond.Broadcast()
		rf.persist()
	}

	// 比较日志是否匹配（本地最后一条日志的索引号和任期号是否匹配）
	if rf.GetLastEntry().Index < args.PrevLogIndex || args.PrevLogTerm != rf.GetLogIndex(args.PrevLogIndex).Term {
		reply.XLen = rf.GetLastEntry().Index // 记录最后一条日志的索引号
		if rf.GetLastEntry().Index >= args.PrevLogIndex {
			reply.XTerm = rf.GetLogIndex(args.PrevLogIndex).Term // 冲突的日志在本地的任期
			reply.XIndex = args.PrevLogIndex // 记录冲突的索引号
			
			//找到冲突的任期的第一条日志
			for reply.XIndex > rf.LastIncludedIndex && rf.GetLogIndex(reply.XIndex).Term == reply.XTerm {
				reply.XIndex--
			}
			reply.XIndex++
		}
		fmt.Printf("[%d %d] AppendEntries fail because of consistence, XLen = %d, XTerm = %d, XIndex = %d", rf.me, rf.State, reply.XLen, reply.XTerm, reply.XIndex)
		reply.Success = false
		return
	}

	// 日志复制
	for index, entry := range args.Entries {
		// 将之后的日志全部覆盖
		if rf.GetLastEntry().Index < entry.Index || entry.Term != rf.GetLogIndex(entry.Index).Term {
			var log []LogEntry
			for i := rf.LastIncludedIndex + 1; i <= entry.Index - 1; i++ { // 将匹配的日志保留
				log = append(log, rf.GetLogIndex(i))
			}
			log = append(log, args.Entries[index:]...) // 不匹配的日志做替换，新的日志做增加
			rf.Log = log
			rf.persist()
			fmt.Printf("[%d %d] Append new log %v", rf.me, rf.State, rf.Log)
		}
	}

	if args.LeaderCommit > rf.CommitIndex {
		rf.CommitIndex = Min(args.LeaderCommit, rf.GetLastEntry().Index)
		fmt.Printf("[%d %d] CommitIndex update to %d\n", rf.me, rf.State, rf.CommitIndex)
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply * InstallSnapshotReply) {
	// pass
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) SendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	_, isLeader := rf.GetState()
	if !isLeader || rf.killed() {
		rf.DPrintf("[%d] Start() Fail isleader = %t, isKilled = %t", rf.me, isLeader, rf.killed())
		return -1, -1, false
	}

	rf.mu.Lock()
	logEntry := LogEntry{Command: command, Term: rf.CurrentTerm, Index: rf.GetLastEntry().Index}
	rf.Log = append(rf.Log, logEntry)
	rf.persist()
	rf.mu.Unlock()
	rf.DPrintf("[%d] Start() ,index : %d,term : %d,command : %d", rf.me, logEntry.Index, logEntry.Term, command)
	atomic.StoreInt32(&rf.quicklyCheck, 20)
	

	// 发送日志
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go rf.SendEntries(i)
		}
	}

	return logEntry.Index, logEntry.Term, isLeader
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//选举
func (rf *Raft) doElection() {
	rf.CurrentTerm++
	voteGranted := 1
	rf.VotedFor = rf.me
	rf.persist()
	rf.ElectionTimeout = GetElectionTimeout() // 重置超时时间
	term := rf.CurrentTerm
	ElectionTimeout := rf.ElectionTimeout
	var lastLogTerm, lastLogIndex int
	lastLogIndex = rf.GetLastEntry().Index
	lastLogTerm = rf.GetLastEntry().Term
	rf.DPrintf("[%d] Start Election,term = %d\n", rf.me, term)
	rf.mu.Unlock()

	// 发起投票
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(server int) {
				args := RequestVoteArgs{Term: term, CandidateId: rf.me, LastLogIndex: lastLogIndex, LastLogTerm: lastLogTerm}
				reply := RequestVoteReply{}
				fmt.Printf("[%d] send RequestVote to server %d\n", rf.me, server)
				if !rf.sendRequestVote(server, &args, &reply) {
					rf.cond.Broadcast()
					return
				}
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.VoteGranted {
					voteGranted++
				}
				// 对方任期更大
				if reply.Term > rf.CurrentTerm {
					rf.CurrentTerm = reply.Term
					rf.persist()
					rf.State = FOLLOWER
					rf.ElectionTimeout = GetElectionTimeout()
				}
				rf.cond.Broadcast()
			}(i)
		}
	}
	var timeout int32
	go func(electionTimeout int, timeout *int32) {
		time.Sleep(time.Duration(electionTimeout) * time.Millisecond)
		atomic.StoreInt32(timeout, 1)
		rf.cond.Broadcast()
	}(ElectionTimeout, &timeout)
	for {
		rf.mu.Lock()
		if voteGranted <= len(rf.peers) / 2 && rf.CurrentTerm == term && 
				rf.State == CANDIDATE && atomic.LoadInt32(&timeout) == 0 {
			rf.cond.Wait()
		}
		if rf.CurrentTerm != term || rf.State != CANDIDATE || atomic.LoadInt32(&timeout) != 0 {
			rf.mu.Unlock()
			break
		}
		if voteGranted > len(rf.peers) / 2 {
			fmt.Printf("[%d] is voted as LEADER(term : %d)\n", rf.me, rf.CurrentTerm)
			rf.State = LEADER
			rf.CommitIndex = 0
			for i := 0; i < len(rf.peers); i++ {
				rf.MatchIndex[i] = 0
				rf.NextIndex[i] = rf.GetLastEntry().Index + 1
			}
			rf.mu.Unlock()
			rf.TrySendEntries(true)
			break
		}
		rf.mu.Unlock()
	}
	fmt.Printf("[%d] Do election finished", rf.me)
}

func (rf *Raft) SendSnapshot(server int) {
	// pass
}

// 向server发送心跳包
func (rf *Raft) SendHeartBeat(server int) {
	rf.mu.Lock()
	fmt.Printf("[%d] send heartBeat to server %d\n", rf.me, server)

	if rf.State != LEADER {
		rf.mu.Unlock()
		return
	}
	if rf.NextIndex[server] <= rf.LastIncludedIndex {
		rf.mu.Unlock()
		return
	}
	term := rf.CurrentTerm
	leaderCommit := rf.CommitIndex
	prevLogIndex := rf.NextIndex[server] - 1
	prevLogTerm := rf.GetLogIndex(prevLogIndex).Term
	rf.mu.Unlock()
	args := AppendEntriesArgs{Term: term, LeaderId: rf.me, LeaderCommit: leaderCommit, PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm}
	reply := AppendEntriesReply{}
	if !rf.sendAppendEntries(server, &args, &reply) {
		return
	}
	rf.mu.Lock()
	if reply.Term > rf.CurrentTerm {
		rf.CurrentTerm = reply.Term
		rf.State = FOLLOWER
		rf.ElectionTimeout = GetElectionTimeout()
		rf.VotedFor = -1
		rf.persist()
		rf.mu.Unlock()
		return
	}

	// 失败后，发送不匹配的所有日志
	if !reply.Success {
		if reply.XLen < prevLogIndex { // 通过XLen可以直接退回
			rf.NextIndex[server] = Max(reply.XLen, 1)
		} else {
			// 找到和reply任期不冲突的
			newNextIndex := prevLogIndex
			for newNextIndex > rf.LastIncludedIndex && rf.GetLogIndex(newNextIndex).Term > reply.XTerm {
				newNextIndex--
			}
			
			if rf.GetLogIndex(newNextIndex).Term == reply.XTerm {
				rf.NextIndex[server] = Max(newNextIndex, rf.LastIncludedIndex + 1) // 6.824 7.3 场景2
			} else {
				rf.NextIndex[server] = reply.XIndex //6.824 7.3 场景1
			}
		}
		fmt.Printf("[%d] sendheartbeat rf.NextIndex[%d] update to %d", rf.me, server, rf.NextIndex[server])
	}
	rf.mu.Unlock()
}

// 向server 发送日志
func (rf *Raft) SendEntries(server int) {
	done := false
	for !done {
		rf.mu.Lock()
		if rf.State != LEADER {
			rf.mu.Unlock()
			return
		}
		if rf.NextIndex[server] <= rf.LastIncludedIndex {
			rf.mu.Unlock()
			return
		}
		done = true
		term := rf.CurrentTerm
		leaderCommit := rf.CommitIndex
		prevLogIndex := rf.NextIndex[server] - 1
		prevLogTerm := rf.GetLogIndex(prevLogIndex).Term
		entries := rf.Log[prevLogIndex - rf.LastIncludedIndex:] // 未应用的Log全部发送
		fmt.Printf("[%d] send Entries to server %d,prevLogIndex = %d,prevLogTerm = %d,LastIncludeIndex = %d\n", rf.me,
		server, prevLogIndex, prevLogTerm, rf.LastIncludedIndex)
		rf.mu.Unlock()
		args := AppendEntriesArgs{Term: term, LeaderId: rf.me, PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm, Entries: entries, LeaderCommit: leaderCommit}
		reply := AppendEntriesReply{}
		if !rf.sendAppendEntries(server, &args, &reply){
			return
		}
		rf.mu.Lock()
		if reply.Term > rf.CurrentTerm {
			rf.CurrentTerm = reply.Term
			rf.State = FOLLOWER
			rf.ElectionTimeout = GetElectionTimeout()
			rf.VotedFor = -1
			rf.persist()
			rf.mu.Unlock()
			return
		}
		if !reply.Success {
			if reply.XLen < prevLogIndex {
				rf.NextIndex[server] = Max(reply.XLen, 1)
			} else {
				newNextIndex := prevLogIndex
				for newNextIndex > rf.LastIncludedIndex && rf.GetLogIndex(newNextIndex).Term > reply.XTerm {
					newNextIndex--
				}
				if rf.GetLogIndex(newNextIndex).Term == reply.XTerm {
					rf.NextIndex[server] = Max(newNextIndex, rf.LastIncludedIndex + 1)
				} else {
					rf.NextIndex[server] = reply.XIndex
				}
			}
			fmt.Printf("[%d] sendentires rf.NextIndex[%d] update to %d", rf.me, server, rf.NextIndex[server])
			done = false
		} else {
			rf.NextIndex[server] = Max(rf.NextIndex[server], prevLogIndex + len(entries) + 1)
			rf.MatchIndex[server] = Max(rf.MatchIndex[server], prevLogIndex + len(entries))
			fmt.Printf("[%d] AppendEntries success,NextIndex is %v,MatchIndex is %v", rf.me, rf.NextIndex, rf.MatchIndex)
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) TrySendEntries(initialize bool) {
	for i := 0; i < len(rf.peers); i++ {
		rf.mu.Lock()
		nextIndex := rf.NextIndex[i]
		firstLogIndex := rf.GetFirstEntry().Index
		lastLogIndex := rf.GetLastEntry().Index
		rf.mu.Unlock()
		if i != rf.me {
			if lastLogIndex >= nextIndex || initialize { // 日志比其他节点或者刚开始初始化
				if firstLogIndex <= nextIndex {
					go rf.SendEntries(i) // 日志复制
				} else {
					go rf.SendSnapshot(i)
				}
			} else {
				go rf.SendHeartBeat(i)
			}
		}
	}
}

// 用于leader统计日志提交是否过半，更新commitIndex
func (rf *Raft) UpdateCommitIndex() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	newCommitIndex := rf.CommitIndex
	for N := rf.CommitIndex + 1; N <= rf.GetLastEntry().Index; N++ {
		if N > rf.LastIncludedIndex && rf.GetLogIndex(N).Term == rf.CurrentTerm {
			count := 1
			for i := 0; i < len(rf.peers); i++ {
				if rf.MatchIndex[i] >= N {
					count++
					if count > len(rf.peers) / 2 {
						newCommitIndex = N
						break
					}
				}
			}
		}
	}
	rf.CommitIndex = newCommitIndex
	fmt.Printf("[%d] Update CommitIndex, term = %d,NextIndex is %v,MatchIndex is %v,CommitIndex is %d", rf.me, rf.CurrentTerm, rf.NextIndex, rf.MatchIndex, rf.CommitIndex)
}

func (rf *Raft) UpdateApplied() {
	rf.LastApplied = Max(rf.LastApplied, rf.LastIncludedIndex)
	for rf.LastApplied < rf. CommitIndex && rf.LastApplied < rf.GetLastEntry().Index && rf.LastApplied >= rf.LastIncludedIndex {
		msg := ApplyMsg{CommandValid: true, Command: rf.GetLogIndex(rf.LastApplied + 1).Command, CommandIndex: rf.LastApplied + 1}
		fmt.Printf("[%d] try apply msg %d", rf.me, rf.LastApplied+1)
		rf.mu.Unlock()
		rf.applych <- msg
		rf.mu.Lock()
		fmt.Printf("[%d] apply msg %d success", rf.me, rf.LastApplied+1)
		rf.LastApplied++
	}
}

// leader任务
func (rf *Raft) leaderTask() {
	rf.mu.Unlock()
	if atomic.LoadInt32(&rf.quicklyCheck) <= 0 {
		rf.TrySendEntries(false)
		rf.UpdateCommitIndex() // 更新commitindex，???为什么不等前面的协程任务跑完
		time.Sleep(time.Duration(rf.BroadcastTime) * time.Millisecond)
	} else {
		rf.UpdateCommitIndex()
		time.Sleep(time.Millisecond)
		atomic.AddInt32(&rf.quicklyCheck, -1)
	}
}
// 每个节点跑的任务
func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()
		rf.UpdateApplied()
		State := rf.State
		if State == LEADER {
			rf.leaderTask()
		} else if State == FOLLOWER {
			fmt.Printf("[%d] ElectionTimeout = %d", rf.me, rf.ElectionTimeout)
			if rf.ElectionTimeout < rf.BroadcastTime {
				rf.State = CANDIDATE
				fmt.Printf("[%d] ElectionTimeout,convert to CANDIDATE\n", rf.me)
				rf.mu.Unlock()
				continue
			}
			rf.ElectionTimeout -= rf.BroadcastTime
			rf.mu.Unlock()
			time.Sleep(time.Duration(rf.BroadcastTime) * time.Millisecond)
		} else if State == CANDIDATE {
			rf.doElection()
		}
	}
}

// 创建一个raft server
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister,
					applyCh chan ApplyMsg, name string) *Raft {
	rf := &Raft{}
	rf.mu.Lock()
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applych = applyCh
	rf.name = name
	rf.cond = sync.NewCond(&rf.mu)
	fmt.Printf("[%d] is Making , len(peers) = %d\n", me, len(peers))
	rf.BroadcastTime = BROADCASTTIME
	rf.ElectionTimeout = GetElectionTimeout()
	rf.State = FOLLOWER
	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.MatchIndex = make([]int, len(peers))
	rf.NextIndex = make([]int, len(peers))
	rf.mu.Unlock()
	rf.readPersist(persister.ReadRaftState())
	go rf.ticker()

	return rf
}
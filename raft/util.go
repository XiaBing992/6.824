/*
 * @Author: WXB 1763567512@qq.com
 * @Date: 2024-05-05 21:53:24
 * @LastEditors: WXB 1763567512@qq.com
 * @LastEditTime: 2024-05-05 22:36:13
 * @FilePath: /6.824/raft/util.go
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
package raft
import (
	"log"
	"math/rand"
	"time"
)

const Debug = false

func (rf *Raft) DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug && rf.name[0] == 'K' {
		log.Printf(rf.name + " " + format, a...)
	}

	return
}

// 如果日志被压缩了，返回快照的记录
func (rf *Raft) GetLastEntry() LogEntry {
	if len(rf.Log) == 0 {
		return LogEntry{Term: rf.LastIncludedTerm, Index: rf.LastIncludedIndex}
	}
	return rf.Log[len(rf.Log) - 1]
}

func (rf *Raft) GetFirstEntry() LogEntry {
	if len(rf.Log) == 0 {
		return LogEntry{Term: rf.LastIncludedTerm, Index: rf.LastIncludedIndex}
	}

	return rf.Log[0]
}

func GetElectionTimeout() int {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return ELECTIONTIMEOUTBASE + int(r.Int31n(ELECTIONTIMEOUTRANGE))
}
func (rf *Raft) GetLogIndex(index int) LogEntry {
	if index == 0 {
		return LogEntry{Term: -1, Index: 0}
	} else if index == rf.LastIncludedIndex {
		return LogEntry{Term: rf.LastIncludedTerm, Index: rf.LastIncludedIndex}
	} else {
		return rf.Log[index - rf.LastIncludedIndex - 1] // 返回对应日志的记录
	}
}

func Min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}
func Max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}
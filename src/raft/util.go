package raft

import (
	"log"
	"os"
)

// Debugging
const Debug = true

func init() {
	logFile, err := os.OpenFile("log.txt", os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		panic(err)
	}
	logFile.Truncate(0)
	log.SetOutput(logFile)

}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Entry struct {
	Term    int
	Command interface{}
}

type Log struct {
	logs       []Entry
	startIndex int
}

func makeEmptyLog() Log {
	return Log{make([]Entry, 1), 0}
}

func (l *Log) firstIndex() int {
	return l.startIndex
}

func (l *Log) lastIndex() int {
	return l.startIndex + len(l.logs) - 1
}

func (l *Log) entry(index int) Entry {
	return l.logs[index-l.firstIndex()]
}

func (l *Log) append(e Entry) {
	l.logs = append(l.logs, e)
}

func (l *Log) AppendLogs(startIndex int, logs []Entry) {
	l.logs = append(l.logs[:startIndex+1-l.firstIndex()], logs...)
}

func (l *Log) preCuted(preIndex int) {
	l.logs = l.logs[:preIndex-l.firstIndex()+1]
}

func (l *Log) nextCuted(nextIndex int) {
	l.startIndex += nextIndex
	l.logs = l.logs[nextIndex-l.firstIndex():]
}

func (l *Log) preSlice(preIndex int) []Entry {
	return l.logs[:preIndex-l.firstIndex()+1]
}

func (l *Log) nextSlice(nextIndex int) []Entry {
	return l.logs[nextIndex-l.firstIndex():]
}

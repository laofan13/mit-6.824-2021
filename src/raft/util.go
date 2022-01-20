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
	Logs       []Entry
	StartIndex int
}

func makeEmptyLog() Log {
	return Log{make([]Entry, 1), 0}
}

func (l *Log) firstIndex() int {
	return l.StartIndex
}

func (l *Log) firstLog() Entry {
	return l.entry(l.firstIndex())
}

func (l *Log) lastIndex() int {
	return l.StartIndex + len(l.Logs) - 1
}

func (l *Log) lastLog() Entry {
	return l.entry(l.lastIndex())
}

func (l *Log) entry(index int) Entry {
	return l.Logs[index-l.firstIndex()]
}

func (l *Log) append(e Entry) {
	l.Logs = append(l.Logs, e)
}

func (l *Log) AppendLogs(StartIndex int, Logs []Entry) {
	l.Logs = append(l.Logs[:StartIndex+1-l.firstIndex()], Logs...)
}

func (l *Log) preCuted(preIndex int) {
	l.Logs = l.Logs[:preIndex-l.firstIndex()+1]
}

func (l *Log) nextCuted(nextIndex int) {
	l.Logs = l.Logs[nextIndex-l.firstIndex():]
	l.StartIndex = nextIndex
	l.Logs[0].Command = nil
}

func (l *Log) preSlice(preIndex int) []Entry {
	return l.Logs[:preIndex-l.firstIndex()+1]
}

func (l *Log) nextSlice(nextIndex int) []Entry {
	return l.Logs[nextIndex-l.firstIndex():]
}

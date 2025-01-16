package raft

import "log"

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func min(i, j int) int {
	if i < j {
		return i
	}
	return j
}

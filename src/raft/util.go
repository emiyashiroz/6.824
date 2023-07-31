package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debug Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func min(n1, n2 int) int {
	if n1 < n2 {
		return n1
	}
	return n2
}

// RandTime 随机时间生成器
func RandTime(min, max int) time.Duration {
	return time.Millisecond * time.Duration(rand.Int()%(max-min)+min)
}

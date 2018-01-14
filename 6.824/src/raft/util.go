package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func AfterBetween(min, max time.Duration) time.Duration {
	rd := rand.New(rand.NewSource(time.Now().UnixNano()))
	d, delta := min, max-min
	if delta > 0 {
		d += time.Duration(rd.Int63n(int64(delta)))
	}
	return d
}

func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

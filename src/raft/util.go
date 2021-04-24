package raft

import (
	"log"
	"strings"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		if Debug > 1 && (strings.Contains(format, "LOCK") || !strings.Contains(format, "]")){
			return
		}
		log.Printf(format, a...)
	}
	return
}

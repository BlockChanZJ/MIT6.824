package raft

import (
	"fmt"
	"log"
	"strings"
)

// Debugging
const Debug = 3
const MDebug = 2

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug == 1 { // log
		log.Printf(format, a...)
	} else if Debug == 2 { // fmt
		if strings.Contains(format, "LOCK") {
			return
		}
		fmt.Printf(format, a...)
	} else if Debug == 3 { // including LOCKs
		fmt.Printf(format, a...)
	}
	return
}
func MPrint(format string, a ...interface{}) (n int, err error) {
	if MDebug == 2 {
		fmt.Printf(format, a...)
	} else if MDebug == 1 {
		DPrintf(format, a...)
	}
	return
}

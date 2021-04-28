package raft

import (
	"fmt"
	"log"
	"strings"
)

// Debugging
const Debug = 3

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

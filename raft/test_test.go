package raft

import (
	"testing"
	"time"
	"fmt"
)

const RaftElectionTimeout = 1000 * time.Millisecond

func TestInitialElection2A(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2A): initial election")

	cfg.checkOneLeader() // 检查是否只有一个leader

	time.Sleep(50 * time.Microsecond)
	term1 := cfg.checkTerms()
	if term1 < 1 {
		t.Fatalf("term is %v, but should be at least 1", term1)
	}

	time.Sleep(2 * RaftElectionTimeout)

	term2 := cfg.checkTerms()
	if term1 != term2 {
		fmt.Printf("warning: term changed even though there were no failures")
	}

	cfg.checkOneLeader()

	cfg.end()
}

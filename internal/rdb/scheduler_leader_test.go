package rdb

import (
	"testing"
	"time"
)

func TestSchedulerLeaderLock(t *testing.T) {
	r := setup(t)
	defer r.Close()

	const name = "cluster_a"
	const inst1 = "inst-1"
	const inst2 = "inst-2"
	lease := 5 * time.Second

	token1, ok, err := r.AcquireSchedulerLeader(name, inst1, lease)
	if err != nil {
		t.Fatalf("AcquireSchedulerLeader(inst1) returned error: %v", err)
	}
	if !ok {
		t.Fatalf("AcquireSchedulerLeader(inst1) ok=false, want true")
	}
	if token1 <= 0 {
		t.Fatalf("AcquireSchedulerLeader(inst1) token=%d, want > 0", token1)
	}

	_, ok2, err := r.AcquireSchedulerLeader(name, inst2, lease)
	if err != nil {
		t.Fatalf("AcquireSchedulerLeader(inst2) returned error: %v", err)
	}
	if ok2 {
		t.Fatalf("AcquireSchedulerLeader(inst2) ok=true while inst1 holds lease, want false")
	}

	isLeader, err := r.IsSchedulerLeader(name, inst1, token1)
	if err != nil {
		t.Fatalf("IsSchedulerLeader(inst1) error: %v", err)
	}
	if !isLeader {
		t.Fatalf("IsSchedulerLeader(inst1, token1)=false, want true")
	}

	isLeader2, err := r.IsSchedulerLeader(name, inst2, token1)
	if err != nil {
		t.Fatalf("IsSchedulerLeader(inst2) error: %v", err)
	}
	if isLeader2 {
		t.Fatalf("IsSchedulerLeader(inst2, token1)=true, want false")
	}

	renewToken, ok, err := r.RenewSchedulerLeader(name, inst1, lease)
	if err != nil {
		t.Fatalf("RenewSchedulerLeader(inst1) error: %v", err)
	}
	if !ok || renewToken != token1 {
		t.Fatalf("RenewSchedulerLeader(inst1)=(%d,%v), want (%d,true)", renewToken, ok, token1)
	}

	if err := r.ReleaseSchedulerLeader(name, inst1); err != nil {
		t.Fatalf("ReleaseSchedulerLeader(inst1) error: %v", err)
	}

	token2, ok, err := r.AcquireSchedulerLeader(name, inst2, lease)
	if err != nil {
		t.Fatalf("AcquireSchedulerLeader(inst2 after release) error: %v", err)
	}
	if !ok {
		t.Fatalf("AcquireSchedulerLeader(inst2 after release) ok=false, want true")
	}
	if token2 != token1+1 {
		t.Fatalf("fencing token after failover = %d, want %d (strictly monotonic)", token2, token1+1)
	}

	staleLeader, err := r.IsSchedulerLeader(name, inst1, token1)
	if err != nil {
		t.Fatalf("IsSchedulerLeader(stale inst1) error: %v", err)
	}
	if staleLeader {
		t.Fatalf("stale leader inst1 with old token1 still reports leader=true, want false (fencing)")
	}
}

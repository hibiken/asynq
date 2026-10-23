package asynq

import (
	"testing"
)

func schedulerFirstJob(s *Scheduler) *enqueueJob {
	entries := s.cron.Entries()
	return entries[0].Job.(*enqueueJob)
}

func TestSchedulerSingleEnqueueAcrossReplicas(t *testing.T) {
	setup(t)
	opt := getRedisConnOpt(t)
	const sharedKey = "group_x"

	s1 := NewScheduler(opt, &SchedulerOpts{SharedKey: sharedKey})
	s2 := NewScheduler(opt, &SchedulerOpts{SharedKey: sharedKey})
	defer s1.client.Close()
	defer s2.client.Close()

	task := NewTask("periodic:sync", nil)
	if _, err := s1.Register("@every 1h", task); err != nil {
		t.Fatalf("s1.Register: %v", err)
	}
	if _, err := s2.Register("@every 1h", task); err != nil {
		t.Fatalf("s2.Register: %v", err)
	}

	s1.acquireOrRenewLeadership()
	s2.acquireOrRenewLeadership()

	if !s1.isLeader {
		t.Fatalf("s1 expected to win leadership first, but isLeader=false")
	}
	if s2.isLeader {
		t.Fatalf("s2 expected to be follower, but isLeader=true")
	}

	schedulerFirstJob(s1).Run()
	schedulerFirstJob(s2).Run()

	insp := NewInspector(opt)
	defer insp.Close()
	pending, err := insp.ListPendingTasks("default")
	if err != nil {
		t.Fatalf("ListPendingTasks: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("two replicas each fired once: got %d enqueued tasks, want exactly 1", len(pending))
	}
}

func TestSchedulerFencingAfterFailover(t *testing.T) {
	setup(t)
	opt := getRedisConnOpt(t)
	const sharedKey = "group_y"

	s1 := NewScheduler(opt, &SchedulerOpts{SharedKey: sharedKey})
	s2 := NewScheduler(opt, &SchedulerOpts{SharedKey: sharedKey})
	defer s1.client.Close()
	defer s2.client.Close()

	task := NewTask("periodic:sync", nil)
	if _, err := s1.Register("@every 1h", task); err != nil {
		t.Fatalf("s1.Register: %v", err)
	}
	if _, err := s2.Register("@every 1h", task); err != nil {
		t.Fatalf("s2.Register: %v", err)
	}

	s1.acquireOrRenewLeadership()
	if !s1.isLeader {
		t.Fatalf("s1 expected to be leader")
	}

	if err := s1.rdb.ReleaseSchedulerLeader(sharedKey, s1.id); err != nil {
		t.Fatalf("release s1 leadership: %v", err)
	}
	s2.acquireOrRenewLeadership()
	if !s2.isLeader {
		t.Fatalf("s2 expected to take over leadership after s1 released")
	}

	insp := NewInspector(opt)
	defer insp.Close()
	pendingCount := func() int {
		p, err := insp.ListPendingTasks("default")
		if err != nil {
			return 0
		}
		return len(p)
	}

	schedulerFirstJob(s1).Run()
	if n := pendingCount(); n != 0 {
		t.Fatalf("stale ex-leader s1 must not enqueue after losing leadership: got %d, want 0", n)
	}

	schedulerFirstJob(s2).Run()
	if n := pendingCount(); n != 1 {
		t.Fatalf("current leader s2 must enqueue exactly once: got %d, want 1", n)
	}
}

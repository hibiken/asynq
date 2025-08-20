package asynq

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/rdb"
	"github.com/hibiken/asynq/internal/testbroker"
	h "github.com/hibiken/asynq/internal/testutil"
)

var (
	cmpSliceIgnoreOrder = cmpopts.SortSlices(func(a, b string) bool {
		return a < b
	})
)

func TestQueueManager(t *testing.T) {
	r := setup(t)
	defer r.Close()
	rdbClient := rdb.NewRDB(r)
	const interval = 500 * time.Millisecond
	qm := newQueueManager(queueManagerParams{
		logger:   testLogger,
		broker:   rdbClient,
		interval: interval,
		queues: map[string]int{
			"high":  10,
			"app:*": 5,
			"low":   2,
			// no "*" to ignore default queue
		},
		strictPriority: true,
		dynamicQueues:  true,
	})
	qm.start(&sync.WaitGroup{})
	defer qm.shutdown()

	var (
		m1 = h.NewTaskMessage("task1", nil) // default queue should be ignored
		m2 = h.NewTaskMessageWithQueue("task3", nil, "high")
		m3 = h.NewTaskMessageWithQueue("task4", nil, "low")
		m4 = h.NewTaskMessageWithQueue("task5", nil, "app:email")
		m5 = h.NewTaskMessageWithQueue("task6", nil, "app:push")
		m6 = h.NewTaskMessage("task2", nil)
	)
	// Enqueue tasks first 3 tasks
	if err := rdbClient.Enqueue(context.Background(), m1); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}
	if err := rdbClient.Enqueue(context.Background(), m2); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}
	if err := rdbClient.Enqueue(context.Background(), m3); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}
	// Wait for one second to update queue manager
	time.Sleep(time.Second)

	expectedQueues := []string{"high", "low"}
	unordered := qm.GetUnorderedQueueNames()
	if diff := cmp.Diff(expectedQueues, unordered, cmpSliceIgnoreOrder); diff != "" {
		t.Errorf("GetUnorderedQueueNames() mismatch (-want +got):\n%s", diff)
	}
	ordered := qm.GetQueuesInDequeueOrder()
	// check 10 times to ensure the order is fixed
	if diff := cmp.Diff(expectedQueues, ordered); diff != "" {
		t.Errorf("GetQueuesInDequeueOrder() mismatch (-want +got):\n%s", diff)
	}
	// Enqueue more tasks
	if err := rdbClient.Enqueue(context.Background(), m4); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}
	if err := rdbClient.Enqueue(context.Background(), m5); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}
	if err := rdbClient.Enqueue(context.Background(), m6); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}
	// Wait for one second to update queue manager
	time.Sleep(time.Second)
	expectedQueues = []string{
		"high",      // 10
		"app:email", // 5  - same priority - can be third
		"app:push",  // 5  - same priority - can be second
		"low",       // 2
	}
	unordered = qm.GetUnorderedQueueNames()
	if diff := cmp.Diff(expectedQueues, unordered, cmpSliceIgnoreOrder); diff != "" {
		t.Fatalf("GetUnorderedQueueNames() mismatch (-want +got):\n%s", diff)
	}
	ordered = qm.GetQueuesInDequeueOrder()
	// check fixed order
	itemsWithFixedOrder := []int{0, 3} // 0 - high, 3 - low
	for _, i := range itemsWithFixedOrder {
		if expectedQueues[i] != ordered[i] {
			t.Fatalf("GetQueuesInDequeueOrder() mismatch (-want +got):\n%s", cmp.Diff(expectedQueues, ordered))
		}
	}
	// check shuffled order for same priority queues
	expected, got := expectedQueues[1:3], ordered[1:3]
	if diff := cmp.Diff(expected, got, cmpSliceIgnoreOrder); diff != "" {
		t.Errorf("GetQueuesInDequeueOrder() mismatch (-want +got):\n%s", diff)
	}
}

func newStaticQueueManagerForTest(queues map[string]int, strict bool) *queueManager {
	return newQueueManager(queueManagerParams{
		logger:         testLogger,
		queues:         queues,
		strictPriority: strict,
		dynamicQueues:  false,
	})
}

func newDynamicQueueManagerForTest(t *testing.T, broker base.Broker, queues map[string]int) *queueManager {
	qm := newQueueManager(queueManagerParams{
		logger:         testLogger,
		queues:         queues,
		strictPriority: true,
		dynamicQueues:  true,
		broker:         broker,
		interval:       time.Millisecond * 500,
	})
	qm.start(&sync.WaitGroup{})
	t.Cleanup(func() {
		qm.shutdown()
	})
	return qm
}

func newDynamicQueueManagerWithMock(t *testing.T, queues map[string]int, mock *queueGetterMock) *queueManager {
	qm := newQueueManager(queueManagerParams{
		logger:         testLogger,
		queues:         queues,
		strictPriority: true,
		dynamicQueues:  true,
		broker:         &testbroker.TestBroker{},
		interval:       time.Millisecond * 500,
	})
	qm.dynamicQeueueGetter = mock.GetQueues
	qm.start(&sync.WaitGroup{})
	t.Cleanup(func() {
		qm.shutdown()
	})
	return qm
}

type queueGetterMock struct {
	queues      map[string]struct{}
	returnError error
}

func (q *queueGetterMock) SetQueues(queues []string) {
	q.queues = make(map[string]struct{}, len(queues))
	for _, queue := range queues {
		q.queues[queue] = struct{}{}
	}
}

func (q *queueGetterMock) GetQueues(ctx context.Context) (map[string]struct{}, error) {
	if q.returnError != nil {
		return nil, q.returnError
	}
	return q.queues, nil
}

func Test_wildcardMatcher(t *testing.T) {
	tests := []struct {
		template       string
		queueName      string
		wantMatched    bool
		wantMatchedLen int
	}{
		// suffix match tests
		{
			// suffix exact
			template:       "test_queue*",
			queueName:      "test_queue",
			wantMatched:    true,
			wantMatchedLen: len("test_queue"),
		},
		{
			// suffix match
			template:       "test_queue*",
			queueName:      "test_queue123",
			wantMatched:    true,
			wantMatchedLen: len("test_queue"),
		},
		{
			// suffix non alphanumeric match
			template:       "?t.e=(s)+t_q:e-u|e[1]{2}*",
			queueName:      "?t.e=(s)+t_q:e-u|e[1]{2}.123",
			wantMatched:    true,
			wantMatchedLen: len("?t.e=(s)+t_q:e-u|e[1]{2}"),
		},
		{
			// suffix non alphanumeric not match
			template:    "?t.e=(s)+t_q:e-u|e[1]{2}*",
			queueName:   "?t.e=(s)+t_q:e-u|e[1]",
			wantMatched: false,
		},
		{
			// suffix less not match
			template:    "test_queue*",
			queueName:   "test_queu",
			wantMatched: false,
		},
		{
			// suffix wrong
			template:    "test_queue*",
			queueName:   "mytest_queue",
			wantMatched: false,
		},
		// prefix match tests
		{
			// prefix exact
			template:       "*test_queue",
			queueName:      "test_queue",
			wantMatched:    true,
			wantMatchedLen: len("test_queue"),
		},
		{
			// prefix match
			template:       "*test_queue",
			queueName:      "123test_queue",
			wantMatched:    true,
			wantMatchedLen: len("test_queue"),
		},
		{
			// prefix non alphanumeric match
			template:       "*?t.e=(s)+t_q:e-u|e[1]{2}",
			queueName:      "123?t.e=(s)+t_q:e-u|e[1]{2}",
			wantMatched:    true,
			wantMatchedLen: len("?t.e=(s)+t_q:e-u|e[1]{2}"),
		},
		{
			// prefix non alphanumeric not match
			template:    "*?t.e=(s)+t_q:e-u|e[1]{2}",
			queueName:   "123?t.e=(s)+t_q:e-u|e[1]{2}.",
			wantMatched: false,
		},
		{
			// prefix less not match
			template:    "*test_queue",
			queueName:   "test_queu",
			wantMatched: false,
		},
		{
			// prefix wrong
			template:    "*test_queue",
			queueName:   "mytest_queue1",
			wantMatched: false,
		},
		// middle match tests
		{
			// middle exact
			template:       "test*queue",
			queueName:      "testqueue",
			wantMatched:    true,
			wantMatchedLen: len("testqueue"),
		},
		{
			// middle match
			template:       "test*queue",
			queueName:      "test_some_queue",
			wantMatched:    true,
			wantMatchedLen: len("testqueue"),
		},
		{
			// middle non alphanumeric match
			template:       "?t.e=(s)+t*q:e-u|e[1]{2}",
			queueName:      "?t.e=(s)+t_q:e-u|e[1]{2}",
			wantMatched:    true,
			wantMatchedLen: len("?t.e=(s)+tq:e-u|e[1]{2}"),
		},
		{
			// middle non alphanumeric not match
			template:    "?t.e=(s)+t*q:e-u|e[1]{2}",
			queueName:   "-?t.e=(s)+t_q:e-u|e[1]",
			wantMatched: false,
		},
		{
			// middle less not match
			template:    "test*queue",
			queueName:   "test_ueue",
			wantMatched: false,
		},
		{
			// middle less not match
			template:    "test*queue",
			queueName:   "tes_queue",
			wantMatched: false,
		},
		{
			// middle wrong
			template:    "test*queue",
			queueName:   "tesT_queue",
			wantMatched: false,
		},
		// sides match tests
		{
			// sides exact
			template:       "*test_queue*",
			queueName:      "test_queue",
			wantMatched:    true,
			wantMatchedLen: len("test_queue"),
		},
		{
			// sides match
			template:       "*test_queue*",
			queueName:      "mytest_queue123",
			wantMatched:    true,
			wantMatchedLen: len("test_queue"),
		},
		{
			// sides non alphanumeric match
			template:       "*?t.e=(s)+t_q:e-u|e[1]{2}*",
			queueName:      "xyz?t.e=(s)+t_q:e-u|e[1]{2}.123",
			wantMatched:    true,
			wantMatchedLen: len("?t.e=(s)+t_q:e-u|e[1]{2}"),
		},
		{
			// sides non alphanumeric not match
			template:    "*?t.e=(s)+t_q:e-u|e[1]{2}*",
			queueName:   "xyz?t.e=()+t_q:e-u|e[1]{2}.123",
			wantMatched: false,
		},
		{
			// sides less not match
			template:    "*test_queue*",
			queueName:   "mytest_queu",
			wantMatched: false,
		},
		{
			// sides less not match
			template:    "*test_queue*",
			queueName:   "est_queue1",
			wantMatched: false,
		},
		{
			// sides wrong
			template:    "*test_queue*",
			queueName:   "mytest_Queue12",
			wantMatched: false,
		},
		// multiple wildcards tests match
		{
			template:       "t*st_qu*ue",
			queueName:      "test_queue",
			wantMatched:    true,
			wantMatchedLen: len("tst_quue"),
		},
		{
			template:       "t*st_qu*ue",
			queueName:      "tst_quue",
			wantMatched:    true,
			wantMatchedLen: len("tst_quue"),
		},
		{
			template:       "t*st_qu*ue",
			queueName:      "t-=^-^=-st_quAAAAue",
			wantMatched:    true,
			wantMatchedLen: len("tst_quue"),
		},
		// multiple wildcards tests non-match
		{
			template:    "t*st_qu*ue",
			queueName:   "est_queue",
			wantMatched: false,
		},
		{
			template:    "t*st_qu*ue",
			queueName:   "tet_queue",
			wantMatched: false,
		},
		{
			template:    "t*st_qu*ue",
			queueName:   "testqueue",
			wantMatched: false,
		},
		{
			template:    "t*st_qu*ue",
			queueName:   "test_qeue",
			wantMatched: false,
		},
		{
			template:    "t*st_qu*ue",
			queueName:   "test_quee",
			wantMatched: false,
		},
		// tests with ":" separator suffix
		{
			template:       "app:critical:*",
			queueName:      "app:critical:id123",
			wantMatched:    true,
			wantMatchedLen: len("app:critical:"),
		},
		{
			template:       "app:critical:*",
			queueName:      "app:critical:id123:email",
			wantMatched:    true,
			wantMatchedLen: len("app:critical:"),
		},
		{
			template:       "app:critical:*",
			queueName:      "app:critical:",
			wantMatched:    true,
			wantMatchedLen: len("app:critical:"),
		},
		{
			template:    "app:critical:*",
			queueName:   "app:critical",
			wantMatched: false,
		},
		{
			template:    "app:critical:*",
			queueName:   "app:criical:id123",
			wantMatched: false,
		},
		{
			template:    "app:critical:*",
			queueName:   "a:app:critical:id123",
			wantMatched: false,
		},
		// tests with ":" separator middle
		{
			template:       "app:*:email",
			queueName:      "app:id123:email",
			wantMatched:    true,
			wantMatchedLen: len("app::email"),
		},
		{
			template:       "app:*:email",
			queueName:      "app:customer1:id123:email",
			wantMatched:    true,
			wantMatchedLen: len("app::email"),
		},
		{
			template:    "app:*:email",
			queueName:   "app:id123:",
			wantMatched: false,
		},
		{
			template:    "app:*:email",
			queueName:   "app:id123:email:123",
			wantMatched: false,
		},
		{
			template:    "app:*:email",
			queueName:   "my:app:id123:email",
			wantMatched: false,
		},
		// tests with ":" separator prefix
		{
			template:       "*:email",
			queueName:      "app:id123:email",
			wantMatched:    true,
			wantMatchedLen: len(":email"),
		},
		{
			template:    "*:email",
			queueName:   "app:id123:email:sent",
			wantMatched: false,
		},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s->%s", tt.template, tt.queueName), func(t *testing.T) {
			w, err := newWildcardMatcher(tt.template, 777)
			if err != nil {
				t.Fatalf("newWildcardMatcher() error = %v", err)
			}
			gotPriority, gotMatchedLen, gotOk := w.match(tt.queueName)
			if gotOk != tt.wantMatched {
				t.Fatalf("wildcardMatcher.match() gotOk = %v, want %v", gotOk, tt.wantMatched)
			}
			if !gotOk {
				// if not matched, we don't care about priority and matched length
				return
			}
			if gotPriority != 777 {
				t.Errorf("wildcardMatcher.match() gotPriority = %v, want %v", gotPriority, 777)
			}
			if gotMatchedLen != tt.wantMatchedLen {
				t.Errorf("wildcardMatcher.match() gotMatchedLen = %v, want %v", gotMatchedLen, tt.wantMatchedLen)
			}
		})
	}
}

func Test_DynamicQueue_Priority(t *testing.T) {
	tests := []struct {
		name         string
		config       map[string]int
		queueName    string
		wantMatch    bool
		wantPriority int
	}{
		{
			name: "exact match",
			config: map[string]int{
				"my_queue123": 11,
				"my_queue":    10, //<-- exact match
				"my_queu":     9,
				"my_que":      8,
				"my_queue*":   7,
				"my_queu*":    6,
				"my*":         5,
				"*my_queu*":   4,
				"*my_queue":   3,
				"*":           2,
			},
			queueName:    "my_queue",
			wantMatch:    true,
			wantPriority: 10,
		},
		{
			name: "longest prefix match",
			config: map[string]int{
				"my_queu*": 9, //<-- longest prefix match
				"my_que*":  8,
				"my*":      7,
				"*":        6,
			},
			queueName:    "my_queue",
			wantMatch:    true,
			wantPriority: 9,
		},
		{
			name: "longest common part match",
			config: map[string]int{
				"a*b":  7, // common part len is 2
				"aa*":  6, // common part len is 2
				"*bb":  5, // common part len is 2
				"aaa*": 4, // common part len is 3 <-- longest
				"*":    3,
			},
			queueName:    "aaaxbb",
			wantMatch:    true,
			wantPriority: 4,
		},
		{
			name: "multiple match with same common part len - undefined",
			config: map[string]int{
				// use the same priority to make test pass. Any of these 3 can win for given queue name
				"a*b": 7, // common part len is 2
				"aa*": 7, // common part len is 2
				"*bb": 7, // common part len is 2
			},
			queueName:    "aaaxbb",
			wantMatch:    true,
			wantPriority: 7,
		},
		{
			name: "no match",
			config: map[string]int{
				"some_prefix*": 9,
				"name":         8,
			},
			queueName: "my_queue",
			wantMatch: false,
		},
		{
			name: "default match",
			config: map[string]int{
				"some_prefix*": 9,
				"name":         8,
				"*":            2,
			},
			queueName:    "my_queue",
			wantMatch:    true,
			wantPriority: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, err := newQueuePriorityMatcher(tt.config)
			if err != nil {
				t.Fatalf("newQueuePriorityMatcher() error = %v", err)
			}
			gotPriority, gotMatch := d.GetPriority(tt.queueName)
			if gotMatch != tt.wantMatch {
				t.Fatalf("queuePriorityMatcher.GetPriority() gotMatch = %v, want %v", gotMatch, tt.wantMatch)
			}
			if !gotMatch {
				// if not matched, we don't care about priority
				return
			}
			if gotPriority != tt.wantPriority {
				t.Errorf("queuePriorityMatcher.GetPriority() gotPriority = %v, want %v", gotPriority, tt.wantPriority)
			}
		})
	}
}

func Test_GetQueueConfig(t *testing.T) {
	tests := []struct {
		queuesConfig   map[string]int
		strictPriority bool
		dynamicQueues  bool
	}{
		{
			queuesConfig: map[string]int{
				"critical": 10,
			},
			strictPriority: true,
			dynamicQueues:  false,
		},
		{
			queuesConfig: map[string]int{
				"critical": 10,
			},
			strictPriority: true,
			dynamicQueues:  true,
		},
	}
	for _, tt := range tests {
		qm := newQueueManager(queueManagerParams{
			queues:         tt.queuesConfig,
			strictPriority: tt.strictPriority,
			dynamicQueues:  tt.dynamicQueues,
			broker:         &testbroker.TestBroker{},
		})
		gotStrict, gotDynamic, gotQueues := qm.GetQueueConfig()
		if gotStrict != tt.strictPriority {
			t.Errorf("queueManager.GetQueueConfig() gotStrict = %v, want %v", gotStrict, tt.strictPriority)
		}
		if gotDynamic != tt.dynamicQueues {
			t.Errorf("queueManager.GetQueueConfig() gotDynamic = %v, want %v", gotDynamic, tt.dynamicQueues)
		}
		if !reflect.DeepEqual(gotQueues, tt.queuesConfig) {
			t.Errorf("queueManager.GetQueueConfig() gotQueues = %v, want %v", gotQueues, tt.queuesConfig)
		}
	}
}

func Test_GetUnorderedQueueNames(t *testing.T) {
	queueGetter := &queueGetterMock{}
	qm := newDynamicQueueManagerWithMock(t, map[string]int{"*": 1}, queueGetter)
	tests := []struct {
		name       string
		setQueues  []string
		wantQueues []string
	}{
		{
			name:       "initial queues",
			setQueues:  []string{"critical", "default", "low"},
			wantQueues: []string{"critical", "default", "low"},
		},
		{
			name:       "remove one queue",
			setQueues:  []string{"critical", "default"},
			wantQueues: []string{"critical", "default"},
		},
		{
			name:       "add a new queue",
			setQueues:  []string{"critical", "default", "new_queue"},
			wantQueues: []string{"critical", "default", "new_queue"},
		},
		{
			name:       "replace a queue",
			setQueues:  []string{"replaced", "default", "new_queue"},
			wantQueues: []string{"replaced", "default", "new_queue"},
		},
	}

	for _, tt := range tests {
		queueGetter.SetQueues(tt.setQueues)
		qm.updateDynamicQueues()
		got := qm.GetUnorderedQueueNames()
		if diff := cmp.Diff(tt.wantQueues, got, cmpSliceIgnoreOrder); diff != "" {
			t.Errorf("%s mismatch: (-want +got):\n%s", tt.name, diff)
		}
	}
}

func Test_DynamicQueue_FixedOrderForDistinctPriorities(t *testing.T) {
	queueGetter := &queueGetterMock{
		queues: map[string]struct{}{
			"queue:a": {},
			"queue:b": {},
			"queue:c": {},
			"queue:d": {},
			"queue:e": {},
			"queue:f": {},
		},
	}
	qm := newDynamicQueueManagerWithMock(t, map[string]int{
		// all queues have different priorities
		"queue:a": 6,
		"queue:b": 5,
		"queue:c": 4,
		"queue:d": 3,
		"queue:e": 2,
		"queue:f": 1,
	}, queueGetter)
	qm.updateDynamicQueues()

	// get queues 100 times to ensure the order is fixed and no shuffling happens
	expectedOrder := []string{"queue:a", "queue:b", "queue:c", "queue:d", "queue:e", "queue:f"}
	for range 100 {
		got := qm.GetQueuesInDequeueOrder()
		if diff := cmp.Diff(expectedOrder, got); diff != "" {
			t.Errorf("DynamicQueue_FixedOrderForDistinctPriorities mismatch: (-want +got):\n%s", diff)
		}
	}
}

func Test_DynamicQueue_SamePriorityQueueShuffled(t *testing.T) {
	queues := map[string]struct{}{
		"critical": {},
		"queue:1":  {},
		"queue:2":  {},
		"queue:3":  {},
		"queue:4":  {},
		"queue:5":  {},
		"queue:6":  {},
		"queue:7":  {},
		"queue:8":  {},
		"queue:9":  {},
		"queue:10": {},
		"queue:11": {},
		"queue:12": {},
		"queue:13": {},
		"queue:14": {},
		"queue:15": {},
		"queue:16": {},
		"queue:17": {},
		"queue:18": {},
		"queue:19": {},
		"queue:20": {},
		"queue:21": {},
		"queue:22": {},
		"queue:23": {},
		"queue:24": {},
		"queue:25": {},
		"queue:26": {},
		"queue:27": {},
		"queue:28": {},
		"queue:29": {},
		"queue:30": {},
		"low":      {},
	}
	expectedShuffled := make([]string, 0, len(queues)-2)
	for queue := range queues {
		if queue != "critical" && queue != "low" {
			expectedShuffled = append(expectedShuffled, queue)
		}
	}

	queueGetter := &queueGetterMock{
		queues: queues,
	}
	qm := newDynamicQueueManagerWithMock(t, map[string]int{
		"critical": 10, //<-- single queue with highest priority
		"queue:*":  6,  //<-- 30 queues with same priority in the middle
		"low":      2,  //<-- single queue with lowest priority
	}, queueGetter)
	qm.updateDynamicQueues()

	// get queues 10 times to ensure the order is shuffled for same priority queues
	const n = 10
	results := make([][]string, n)
	for i := range n {
		results[i] = qm.GetQueuesInDequeueOrder()
		if len(results[i]) != len(queues) {
			t.Fatalf("expected %d queues, got %d", len(queues), len(results[i]))
		}
		if results[i][0] != "critical" {
			t.Errorf("expected first queue to be 'critical', got %s", results[i][0])
		}
		if results[i][len(results[i])-1] != "low" {
			t.Errorf("expected last queue to be 'low', got %s", results[i][len(results[i])-1])
		}
		// check if the middle queues are shuffled
		got := results[i][1 : len(results[i])-1] // exclude first and last queues
		if diff := cmp.Diff(expectedShuffled, got, cmpSliceIgnoreOrder); diff != "" {
			t.Errorf("mismatch in iteration %d: (-want +got):\n%s", i, diff)
		}
		if i < n/2 {
			continue
		}
		// for the last half of iterations calculate the number of exact matches with previous iterations
		orderDifferent := 0
		for j := i - 1; j >= 0; j-- {
			if !reflect.DeepEqual(results[i], results[j]) {
				orderDifferent++
			}
		}
		t.Logf("iteration %d: order shuffled in %d/%d", i, orderDifferent, i)
		// we expect that at least one different order appears in previous iterations
		if orderDifferent == 0 {
			t.Errorf("expected at least one different order in the last %d iterations, got %d same orders", i, i)
		}
	}
}

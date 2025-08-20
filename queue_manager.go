package asynq

import (
	"context"
	"fmt"
	"math/rand/v2"
	"regexp"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/log"
)

type queueManager struct {
	logger  *log.Logger
	broker  base.Broker
	matcher *queuePriorityMatcher

	// channel to communicate back to the long running "queueManager" goroutine.
	done chan struct{}

	// params for static queues
	queuesConfig     map[string]int // queue priorities from config
	queueNames       []string       // unordered list of queue names from config
	orderedQueues    []string       // ordered list of queue names for dequeueing tasks.
	normalizedQueues map[string]int // ordered list of queue names for dequeueing tasks.

	strictPriority bool
	dynamicQueues  bool

	// function to get dynamic queues from the broker
	dynamicQeueueGetter func(ctx context.Context) (map[string]struct{}, error)

	// params for dynamic queues
	interval               time.Duration    // interval between dynamic queue updates.
	rw                     sync.RWMutex     //guards dynamic queues and priorities
	dynamicQueueIdx        map[string]int   // index priority by queue name
	dynamicPriorityIdx     map[int][]string // reverse index of queue names by priority
	dynamicPrioritiesOrder []int            // maintains the order of priorities (highest to lowest)
}

// GetQueueConfig returns the current queue configuration.
func (qm *queueManager) GetQueueConfig() (strict, dynamic bool, queues map[string]int) {
	return qm.strictPriority, qm.dynamicQueues, qm.queuesConfig
}

// GetUnorderedQueueNames returns unordered set of queues from the broker.
func (qm *queueManager) GetUnorderedQueueNames() []string {
	if !qm.dynamicQueues {
		return qm.queueNames
	}
	qm.rw.RLock()
	defer qm.rw.RUnlock()
	queues := make([]string, 0, len(qm.dynamicQueueIdx))
	for queue := range qm.dynamicQueueIdx {
		queues = append(queues, queue)
	}
	return queues
}

func (qm *queueManager) GetQueuesInDequeueOrder() []string {
	if !qm.dynamicQueues {
		// use old implementation for static queues
		return qm.dequeueStatic()
	}
	return qm.dequeueDynamic()
}

// moved from processor.go
func (qm *queueManager) dequeueStatic() []string {
	// skip the overhead of generating a list of queue names
	// if we are processing one queue.
	if len(qm.queuesConfig) == 1 {
		return qm.queueNames
	}
	if qm.orderedQueues != nil {
		return qm.orderedQueues
	}
	var names []string
	for qname, priority := range qm.normalizedQueues {
		for i := 0; i < priority; i++ {
			names = append(names, qname)
		}
	}
	rand.Shuffle(len(names), func(i, j int) { names[i], names[j] = names[j], names[i] })
	return uniq(names, len(qm.normalizedQueues))
}

func (qm *queueManager) dequeueDynamic() []string {
	qm.rw.RLock()
	defer qm.rw.RUnlock()
	queues := make([]string, 0, len(qm.dynamicQueueIdx))

	// we go over all queues from the highest priority to the lowest
	// if we have multiple queues with the same priority, we shuffle them
	for _, priority := range qm.dynamicPrioritiesOrder {
		queueNames := qm.dynamicPriorityIdx[priority]
		if len(queueNames) == 1 {
			// if there is only one queue with this priority, add it directly
			queues = append(queues, queueNames[0])
			continue
		}
		oldLen := len(queues)
		// first copy values to queues and then shuffle them in the copied sub-slice
		queues = append(queues, queueNames...)
		// shuffle sub-slice of the queues with the same priority
		rand.Shuffle(len(queueNames), func(i, j int) { queues[oldLen+i], queues[oldLen+j] = queues[oldLen+j], queues[oldLen+i] })
	}
	return queues
}

type queueManagerParams struct {
	logger         *log.Logger
	broker         base.Broker
	interval       time.Duration
	queues         map[string]int
	strictPriority bool
	dynamicQueues  bool
}

func newQueueManager(params queueManagerParams) *queueManager {
	qm := &queueManager{
		logger:   params.logger,
		broker:   params.broker,
		done:     make(chan struct{}),
		interval: params.interval,

		matcher:        nil,
		queuesConfig:   params.queues,
		queueNames:     make([]string, 0, len(params.queues)),
		strictPriority: params.strictPriority,
		dynamicQueues:  params.dynamicQueues,
	}
	// init params for static queues (copied from processor.go)
	for queueName := range params.queues {
		qm.queueNames = append(qm.queueNames, queueName)
	}
	qm.normalizedQueues = normalizeQueues(params.queues)
	if params.strictPriority {
		qm.orderedQueues = sortByPriority(qm.normalizedQueues)
	}
	// init params for dynamic queues
	if params.dynamicQueues {
		if !params.strictPriority {
			panic("invalid configuration: dynamic queues must be used with strict priority enabled")
		}
		if len(params.queues) == 0 {
			panic("invalid configuration: dynamic queues must have at least one queue defined")
		}
		matcher, err := newQueuePriorityMatcher(params.queues)
		if err != nil {
			panic(fmt.Sprintf("failed to create queue priority matcher: %v", err))
		}
		qm.matcher = matcher
		qm.dynamicQueueIdx = map[string]int{}
		qm.dynamicPriorityIdx = make(map[int][]string, len(params.queues))
		qm.dynamicQeueueGetter = params.broker.SetOfQueues
	}
	return qm
}

func (qm *queueManager) shutdown() {
	if !qm.dynamicQueues {
		return
	}
	qm.logger.Debug("QueueManager shutting down...")
	// Signal the queue manager goroutine to stop.
	qm.done <- struct{}{}
}

func (qm *queueManager) start(wg *sync.WaitGroup) {
	if !qm.dynamicQueues {
		return
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		timer := time.NewTimer(qm.interval)
		for {
			qm.updateDynamicQueues()
			select {
			case <-qm.done:
				qm.logger.Debug("QueueManager done")
				timer.Stop()
				return
			case <-timer.C:
				timer.Reset(qm.interval)
			}
		}
	}()
}

func (qm *queueManager) updateDynamicQueues() {
	// get the current set of queues from the broker
	queues, err := qm.dynamicQeueueGetter(context.Background())
	if err != nil {
		qm.logger.Errorf("Failed to get queues: %v", err)
		return
	}
	qm.rw.Lock()
	defer qm.rw.Unlock()

	// remove queues that are no longer present in the broker
	toRemove := make(map[string]int)
	for queue, priority := range qm.dynamicQueueIdx {
		if _, ok := queues[queue]; !ok {
			toRemove[queue] = priority
		}
	}
	for queue, priority := range toRemove {
		// delete from queue index
		delete(qm.dynamicQueueIdx, queue)

		// delete from priority index
		priorities := qm.dynamicPriorityIdx[priority]
		qm.dynamicPriorityIdx[priority] = slices.DeleteFunc(priorities, func(q string) bool {
			return q == queue
		})
	}

	// add new queues from the broker
	updatePriorityOrder := false
	for queue := range queues {
		if _, ok := qm.dynamicQueueIdx[queue]; ok {
			// queue already exists, skip
			continue
		}
		// calculate priority for the new queue and add it to the queue index
		priority, ok := qm.matcher.GetPriority(queue)
		if !ok {
			continue
		}
		qm.dynamicQueueIdx[queue] = priority

		_, priorityExists := qm.dynamicPriorityIdx[priority]
		if !priorityExists {
			// add new priority value to priorities order array
			qm.dynamicPrioritiesOrder = append(qm.dynamicPrioritiesOrder, priority)
			// set flag to sort array later
			updatePriorityOrder = true
		}
		// add queue to priority index
		qm.dynamicPriorityIdx[priority] = append(qm.dynamicPriorityIdx[priority], queue)
	}
	// sort priorities in descending order
	if updatePriorityOrder {
		sort.Slice(qm.dynamicPrioritiesOrder, func(i, j int) bool {
			return qm.dynamicPrioritiesOrder[i] > qm.dynamicPrioritiesOrder[j]
		})
		qm.dynamicPrioritiesOrder = slices.Compact(qm.dynamicPrioritiesOrder)
	}
}

// newQueuePriorityMatcher creates a new queue priority matcher based on the provided dynamic queues.
// Example:
//
//	Queues: map[string]int{
//	    "my-queue-name":    7, // exact queue name
//	    "other-queue-name": 7, // exact queue name. Same priority as "my-queue-name"
//	    "my-queue-*":       6, // matches any queue name that starts with "my-queue-"
//	    "my-queue-low-*":   5, // matches any queue name that starts with "my-queue-low-"
//	                           // longest common prefix will be used.
//	    "*-my-queue":       4, // matches any queue name that ends with "-my-queue"
//	    "*-my-queue-*":     3, // matches any queue name that contains "-my-queue-" in the middle
//	    "my-*-queue":       2, // matches any queue name starting with "my-" and ending with "-queue"
//	    "*":                1, // default priority. Will match any queue name if set.
//	                           // if default priority is not set, not matching queue will be ignored.
//	},
func newQueuePriorityMatcher(dynamicQueues map[string]int) (*queuePriorityMatcher, error) {
	if len(dynamicQueues) == 0 {
		return nil, fmt.Errorf("dynamic queues cannot be empty")
	}
	q := &queuePriorityMatcher{
		exact:    make(map[string]int), // exact queue names without wildcards
		wildcard: nil,                  // wildcard queue names patterns
	}
	// init queue matcher
	for queueName, priority := range dynamicQueues {
		hasWildcard := strings.Contains(queueName, "*")
		if !hasWildcard {
			// queueName is an exact match
			q.exact[queueName] = priority
			continue
		}
		// queueName contains wildcards (*)
		wildcard, err := newWildcardMatcher(queueName, priority)
		if err != nil {
			return nil, err
		}
		q.wildcard = append(q.wildcard, wildcard)
	}
	return q, nil
}

type queuePriorityMatcher struct {
	exact    map[string]int
	wildcard []wildcardMatcher
}

func (d *queuePriorityMatcher) GetPriority(queueName string) (int, bool) {
	if priority, ok := d.exact[queueName]; ok {
		// return priority for exact match
		return priority, true
	}
	// find the longest match in wildcard patterns
	longestMatchLen, matchedPriority := -1, 0
	for _, matcher := range d.wildcard {
		priority, matchedLen, ok := matcher.match(queueName)
		if !ok {
			continue
		}
		if matchedLen > longestMatchLen {
			longestMatchLen = matchedLen
			matchedPriority = priority
		}
	}
	if longestMatchLen == -1 {
		// match not found
		return 0, false
	}
	return matchedPriority, true
}

func newWildcardMatcher(queueName string, priority int) (wildcardMatcher, error) {
	// convert queueName with wildcards (*) to regex
	regexPattern := "^" + regexp.QuoteMeta(queueName) + "$"
	regexPattern = strings.ReplaceAll(regexPattern, `\*`, ".*")
	re, err := regexp.Compile(regexPattern)
	if err != nil {
		return wildcardMatcher{}, fmt.Errorf("failed to compile regex for queue name %s: %w", queueName, err)
	}
	wildcardCount := strings.Count(queueName, "*")
	return wildcardMatcher{
		regexPattern: re,
		exactPartLen: utf8.RuneCountInString(queueName) - wildcardCount,
		priority:     priority,
	}, nil
}

type wildcardMatcher struct {
	regexPattern *regexp.Regexp // regex pattern for matching queue names with wildcards
	exactPartLen int            // number of matching characters excluding wildcards. Used to determine the longest match
	priority     int            // priority of the queue
}

func (w *wildcardMatcher) match(queueName string) (priority, matchedLen int, ok bool) {
	if w.regexPattern.MatchString(queueName) {
		return w.priority, w.exactPartLen, true
	}
	return 0, 0, false
}

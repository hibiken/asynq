// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/errors"
	"github.com/hibiken/asynq/internal/rdb"
	"github.com/redis/go-redis/v9"
)

// A Client is responsible for scheduling tasks.
//
// A Client is used to register tasks that should be processed
// immediately or some time in the future.
//
// Clients are safe for concurrent use by multiple goroutines.
type Client struct {
	broker base.Broker
	// When a Client has been created with an existing Redis connection, we do
	// not want to close it.
	sharedConnection bool
}

// NewClient returns a new Client instance given a redis connection option.
func NewClient(r RedisConnOpt) *Client {
	redisClient, ok := r.MakeRedisClient().(redis.UniversalClient)
	if !ok {
		panic(fmt.Sprintf("asynq: unsupported RedisConnOpt type %T", r))
	}
	client := NewClientFromRedisClient(redisClient)
	client.sharedConnection = false
	return client
}

// NewClientFromRedisClient returns a new instance of Client given a redis.UniversalClient
// Warning: The underlying redis connection pool will not be closed by Asynq, you are responsible for closing it.
func NewClientFromRedisClient(c redis.UniversalClient) *Client {
	return &Client{broker: rdb.NewRDB(c), sharedConnection: true}
}

type OptionType int

const (
	MaxRetryOpt OptionType = iota
	QueueOpt
	TimeoutOpt
	DeadlineOpt
	UniqueOpt
	ProcessAtOpt
	ProcessInOpt
	TaskIDOpt
	RetentionOpt
	GroupOpt
)

// Option specifies the task processing behavior.
type Option interface {
	// String returns a string representation of the option.
	String() string

	// Type describes the type of the option.
	Type() OptionType

	// Value returns a value used to create this option.
	Value() interface{}
}

// Internal option representations.
type (
	retryOption     int
	queueOption     string
	taskIDOption    string
	timeoutOption   time.Duration
	deadlineOption  time.Time
	uniqueOption    time.Duration
	processAtOption time.Time
	processInOption time.Duration
	retentionOption time.Duration
	groupOption     string
)

// MaxRetry returns an option to specify the max number of times
// the task will be retried.
//
// Negative retry count is treated as zero retry.
func MaxRetry(n int) Option {
	if n < 0 {
		n = 0
	}
	return retryOption(n)
}

func (n retryOption) String() string     { return fmt.Sprintf("MaxRetry(%d)", int(n)) }
func (n retryOption) Type() OptionType   { return MaxRetryOpt }
func (n retryOption) Value() interface{} { return int(n) }

// Queue returns an option to specify the queue to enqueue the task into.
func Queue(name string) Option {
	return queueOption(name)
}

func (name queueOption) String() string     { return fmt.Sprintf("Queue(%q)", string(name)) }
func (name queueOption) Type() OptionType   { return QueueOpt }
func (name queueOption) Value() interface{} { return string(name) }

// TaskID returns an option to specify the task ID.
func TaskID(id string) Option {
	return taskIDOption(id)
}

func (id taskIDOption) String() string     { return fmt.Sprintf("TaskID(%q)", string(id)) }
func (id taskIDOption) Type() OptionType   { return TaskIDOpt }
func (id taskIDOption) Value() interface{} { return string(id) }

// Timeout returns an option to specify how long a task may run.
// If the timeout elapses before the Handler returns, then the task
// will be retried.
//
// Zero duration means no limit.
//
// If there's a conflicting Deadline option, whichever comes earliest
// will be used.
func Timeout(d time.Duration) Option {
	return timeoutOption(d)
}

func (d timeoutOption) String() string     { return fmt.Sprintf("Timeout(%v)", time.Duration(d)) }
func (d timeoutOption) Type() OptionType   { return TimeoutOpt }
func (d timeoutOption) Value() interface{} { return time.Duration(d) }

// Deadline returns an option to specify the deadline for the given task.
// If it reaches the deadline before the Handler returns, then the task
// will be retried.
//
// If there's a conflicting Timeout option, whichever comes earliest
// will be used.
func Deadline(t time.Time) Option {
	return deadlineOption(t)
}

func (t deadlineOption) String() string {
	return fmt.Sprintf("Deadline(%v)", time.Time(t).Format(time.UnixDate))
}
func (t deadlineOption) Type() OptionType   { return DeadlineOpt }
func (t deadlineOption) Value() interface{} { return time.Time(t) }

// Unique returns an option to enqueue a task only if the given task is unique.
// Task enqueued with this option is guaranteed to be unique within the given ttl.
// Once the task gets processed successfully or once the TTL has expired,
// another task with the same uniqueness may be enqueued.
// ErrDuplicateTask error is returned when enqueueing a duplicate task.
// TTL duration must be greater than or equal to 1 second.
//
// Uniqueness of a task is based on the following properties:
//   - Task Type
//   - Task Payload
//   - Queue Name
func Unique(ttl time.Duration) Option {
	return uniqueOption(ttl)
}

func (ttl uniqueOption) String() string     { return fmt.Sprintf("Unique(%v)", time.Duration(ttl)) }
func (ttl uniqueOption) Type() OptionType   { return UniqueOpt }
func (ttl uniqueOption) Value() interface{} { return time.Duration(ttl) }

// ProcessAt returns an option to specify when to process the given task.
//
// If there's a conflicting ProcessIn option, the last option passed to Enqueue overrides the others.
func ProcessAt(t time.Time) Option {
	return processAtOption(t)
}

func (t processAtOption) String() string {
	return fmt.Sprintf("ProcessAt(%v)", time.Time(t).Format(time.UnixDate))
}
func (t processAtOption) Type() OptionType   { return ProcessAtOpt }
func (t processAtOption) Value() interface{} { return time.Time(t) }

// ProcessIn returns an option to specify when to process the given task relative to the current time.
//
// If there's a conflicting ProcessAt option, the last option passed to Enqueue overrides the others.
func ProcessIn(d time.Duration) Option {
	return processInOption(d)
}

func (d processInOption) String() string     { return fmt.Sprintf("ProcessIn(%v)", time.Duration(d)) }
func (d processInOption) Type() OptionType   { return ProcessInOpt }
func (d processInOption) Value() interface{} { return time.Duration(d) }

// Retention returns an option to specify the duration of retention period for the task.
// If this option is provided, the task will be stored as a completed task after successful processing.
// A completed task will be deleted after the specified duration elapses.
func Retention(d time.Duration) Option {
	return retentionOption(d)
}

func (ttl retentionOption) String() string     { return fmt.Sprintf("Retention(%v)", time.Duration(ttl)) }
func (ttl retentionOption) Type() OptionType   { return RetentionOpt }
func (ttl retentionOption) Value() interface{} { return time.Duration(ttl) }

// Group returns an option to specify the group used for the task.
// Tasks in a given queue with the same group will be aggregated into one task before passed to Handler.
func Group(name string) Option {
	return groupOption(name)
}

func (name groupOption) String() string     { return fmt.Sprintf("Group(%q)", string(name)) }
func (name groupOption) Type() OptionType   { return GroupOpt }
func (name groupOption) Value() interface{} { return string(name) }

// ErrDuplicateTask indicates that the given task could not be enqueued since it's a duplicate of another task.
//
// ErrDuplicateTask error only applies to tasks enqueued with a Unique option.
var ErrDuplicateTask = errors.New("task already exists")

// ErrTaskIDConflict indicates that the given task could not be enqueued since its task ID already exists.
//
// ErrTaskIDConflict error only applies to tasks enqueued with a TaskID option.
var ErrTaskIDConflict = errors.New("task ID conflicts with another task")

type option struct {
	retry     int
	queue     string
	taskID    string
	timeout   time.Duration
	deadline  time.Time
	uniqueTTL time.Duration
	processAt time.Time
	retention time.Duration
	group     string
}

// composeOptions merges user provided options into the default options
// and returns the composed option.
// It also validates the user provided options and returns an error if any of
// the user provided options fail the validations.
func composeOptions(opts ...Option) (option, error) {
	res := option{
		retry:     defaultMaxRetry,
		queue:     base.DefaultQueueName,
		taskID:    uuid.NewString(),
		timeout:   0, // do not set to defaultTimeout here
		deadline:  time.Time{},
		processAt: time.Now(),
	}
	for _, opt := range opts {
		switch opt := opt.(type) {
		case retryOption:
			res.retry = int(opt)
		case queueOption:
			qname := string(opt)
			if err := base.ValidateQueueName(qname); err != nil {
				return option{}, err
			}
			res.queue = qname
		case taskIDOption:
			id := string(opt)
			if isBlank(id) {
				return option{}, errors.New("task ID cannot be empty")
			}
			res.taskID = id
		case timeoutOption:
			res.timeout = time.Duration(opt)
		case deadlineOption:
			res.deadline = time.Time(opt)
		case uniqueOption:
			ttl := time.Duration(opt)
			if ttl < 1*time.Second {
				return option{}, errors.New("Unique TTL cannot be less than 1s")
			}
			res.uniqueTTL = ttl
		case processAtOption:
			res.processAt = time.Time(opt)
		case processInOption:
			res.processAt = time.Now().Add(time.Duration(opt))
		case retentionOption:
			res.retention = time.Duration(opt)
		case groupOption:
			key := string(opt)
			if isBlank(key) {
				return option{}, errors.New("group key cannot be empty")
			}
			res.group = key
		default:
			// ignore unexpected option
		}
	}
	return res, nil
}

// isBlank returns true if the given s is empty or consist of all whitespaces.
func isBlank(s string) bool {
	return strings.TrimSpace(s) == ""
}

const (
	// Default max retry count used if nothing is specified.
	defaultMaxRetry = 25

	// Default timeout used if both timeout and deadline are not specified.
	defaultTimeout = 30 * time.Minute
)

// Value zero indicates no timeout and no deadline.
var (
	noTimeout  time.Duration = 0
	noDeadline time.Time     = time.Unix(0, 0)
)

// Close closes the connection with redis.
func (c *Client) Close() error {
	if c.sharedConnection {
		return fmt.Errorf("redis connection is shared so the Client can't be closed through asynq")
	}
	return c.broker.Close()
}

// Enqueue enqueues the given task to a queue.
//
// Enqueue returns TaskInfo and nil error if the task is enqueued successfully, otherwise returns a non-nil error.
//
// The argument opts specifies the behavior of task processing.
// If there are conflicting Option values the last one overrides others.
// Any options provided to NewTask can be overridden by options passed to Enqueue.
// By default, max retry is set to 25 and timeout is set to 30 minutes.
//
// If no ProcessAt or ProcessIn options are provided, the task will be pending immediately.
//
// Enqueue uses context.Background internally; to specify the context, use EnqueueContext.
func (c *Client) Enqueue(task *Task, opts ...Option) (*TaskInfo, error) {
	return c.EnqueueContext(context.Background(), task, opts...)
}

// EnqueueContext enqueues the given task to a queue.
//
// EnqueueContext returns TaskInfo and nil error if the task is enqueued successfully, otherwise returns a non-nil error.
//
// The argument opts specifies the behavior of task processing.
// If there are conflicting Option values the last one overrides others.
// Any options provided to NewTask can be overridden by options passed to Enqueue.
// By default, max retry is set to 25 and timeout is set to 30 minutes.
//
// If no ProcessAt or ProcessIn options are provided, the task will be pending immediately.
//
// The first argument context applies to the enqueue operation. To specify task timeout and deadline, use Timeout and Deadline option instead.
func (c *Client) EnqueueContext(ctx context.Context, task *Task, opts ...Option) (*TaskInfo, error) {
	if task == nil {
		return nil, fmt.Errorf("task cannot be nil")
	}
	if strings.TrimSpace(task.Type()) == "" {
		return nil, fmt.Errorf("task typename cannot be empty")
	}
	// merge task options with the options provided at enqueue time.
	opts = append(task.opts, opts...)
	opt, err := composeOptions(opts...)
	if err != nil {
		return nil, err
	}
	deadline := noDeadline
	if !opt.deadline.IsZero() {
		deadline = opt.deadline
	}
	timeout := noTimeout
	if opt.timeout != 0 {
		timeout = opt.timeout
	}
	if deadline.Equal(noDeadline) && timeout == noTimeout {
		// If neither deadline nor timeout are set, use default timeout.
		timeout = defaultTimeout
	}
	var uniqueKey string
	if opt.uniqueTTL > 0 {
		uniqueKey = base.UniqueKey(opt.queue, task.Type(), task.Payload())
	}
	msg := &base.TaskMessage{
		ID:        opt.taskID,
		Type:      task.Type(),
		Payload:   task.Payload(),
		Queue:     opt.queue,
		Retry:     opt.retry,
		Deadline:  deadline.Unix(),
		Timeout:   int64(timeout.Seconds()),
		UniqueKey: uniqueKey,
		GroupKey:  opt.group,
		Retention: int64(opt.retention.Seconds()),
	}
	now := time.Now()
	var state base.TaskState
	if opt.processAt.After(now) {
		err = c.schedule(ctx, msg, opt.processAt, opt.uniqueTTL)
		state = base.TaskStateScheduled
	} else if opt.group != "" {
		// Use zero value for processAt since we don't know when the task will be aggregated and processed.
		opt.processAt = time.Time{}
		err = c.addToGroup(ctx, msg, opt.group, opt.uniqueTTL)
		state = base.TaskStateAggregating
	} else {
		opt.processAt = now
		err = c.enqueue(ctx, msg, opt.uniqueTTL)
		state = base.TaskStatePending
	}
	switch {
	case errors.Is(err, errors.ErrDuplicateTask):
		return nil, fmt.Errorf("%w", ErrDuplicateTask)
	case errors.Is(err, errors.ErrTaskIdConflict):
		return nil, fmt.Errorf("%w", ErrTaskIDConflict)
	case err != nil:
		return nil, err
	}
	return newTaskInfo(msg, state, opt.processAt, nil), nil
}

// EnqueueBatch enqueues multiple tasks to their respective queues atomically.
//
// EnqueueBatch returns a slice of TaskInfo and nil error if all tasks are enqueued successfully.
// If any task fails to enqueue, the function returns the partial results and an error.
// The order of the returned TaskInfo slice matches the order of the input tasks.
//
// The argument opts specifies the behavior of task processing and applies to all tasks in the batch.
// If there are conflicting Option values, the last one overrides others.
// Any options provided to NewTask can be overridden by options passed to EnqueueBatch.
// By default, max retry is set to 25 and timeout is set to 30 minutes.
//
// If no ProcessAt or ProcessIn options are provided, the tasks will be pending immediately.
//
// EnqueueBatch uses context.Background internally; to specify the context, use EnqueueBatchContext.
func (c *Client) EnqueueBatch(tasks []*Task, opts ...Option) ([]*TaskInfo, error) {
	return c.EnqueueBatchContext(context.Background(), tasks, opts...)
}

// EnqueueBatchContext enqueues multiple tasks to their respective queues atomically with the given context.
//
// EnqueueBatchContext returns a slice of TaskInfo and nil error if all tasks are enqueued successfully.
// If any task fails to enqueue, the function returns the partial results and an error.
// The order of the returned TaskInfo slice matches the order of the input tasks.
//
// The argument opts specifies the behavior of task processing and applies to all tasks in the batch.
// If there are conflicting Option values, the last one overrides others.
// Any options provided to NewTask can be overridden by options passed to EnqueueBatchContext.
// By default, max retry is set to 25 and timeout is set to 30 minutes.
//
// The context argument applies to the enqueue operation. To specify task timeout and deadline,
// use Timeout and Deadline options instead.
func (c *Client) EnqueueBatchContext(ctx context.Context, tasks []*Task, opts ...Option) ([]*TaskInfo, error) {
	if len(tasks) == 0 {
		return nil, nil
	}

	// Validate all tasks first
	for i, task := range tasks {
		if task == nil {
			return nil, fmt.Errorf("task at index %d is nil", i)
		}
		if strings.TrimSpace(task.Type()) == "" {
			return nil, fmt.Errorf("task type at index %d cannot be empty", i)
		}
	}

	// Compose options once for all tasks
	opt, err := composeOptions(opts...)
	if err != nil {
		return nil, err
	}

	// Prepare task messages and their corresponding options
	var msgs []*base.TaskMessage
	var taskOpts []option
	now := time.Now()

	for _, task := range tasks {
		// Merge task-specific options with the batch options
		taskOpt := opt
		if len(task.opts) > 0 {
			taskOpt, err = composeOptions(append(task.opts, opts...)...)
			if err != nil {
				return nil, fmt.Errorf("invalid options for task %q: %v", task.Type(), err)
			}
		}

		// Set default values if not specified
		deadline := noDeadline
		if !taskOpt.deadline.IsZero() {
			deadline = taskOpt.deadline
		}

		timeout := noTimeout
		if taskOpt.timeout != 0 {
			timeout = taskOpt.timeout
		}
		if deadline.Equal(noDeadline) && timeout == noTimeout {
			timeout = defaultTimeout
		}

		// Generate unique key if needed
		var uniqueKey string
		if taskOpt.uniqueTTL > 0 {
			uniqueKey = base.UniqueKey(taskOpt.queue, task.Type(), task.Payload())
		}

		// Create task message
		msg := &base.TaskMessage{
			ID:        taskOpt.taskID,
			Type:      task.Type(),
			Payload:   task.Payload(),
			Queue:     taskOpt.queue,
			Retry:     taskOpt.retry,
			Deadline:  deadline.Unix(),
			Timeout:   int64(timeout.Seconds()),
			UniqueKey: uniqueKey,
			GroupKey:  taskOpt.group,
			Retention: int64(taskOpt.retention.Seconds()),
		}

		msgs = append(msgs, msg)
		taskOpts = append(taskOpts, taskOpt)
	}

	// Enqueue all tasks in a batch
	var results []*TaskInfo
	var enqueueErrs []error

	// Group messages by their target (regular, scheduled, or group)
	var regularMsgs, groupMsgs []*base.TaskMessage
	var scheduledMsgs []*scheduledTask

	for i, msg := range msgs {
		opt := taskOpts[i]
		if !opt.processAt.IsZero() && opt.processAt.After(now) {
			scheduledMsgs = append(scheduledMsgs, &scheduledTask{msg: msg, processAt: opt.processAt})
		} else if opt.group != "" {
			groupMsgs = append(groupMsgs, msg)
		} else {
			regularMsgs = append(regularMsgs, msg)
		}
	}

	// Process regular messages
	if len(regularMsgs) > 0 {
		// Check if we need to use unique enqueue
		if taskOpts[0].uniqueTTL > 0 {
			// Use batch unique enqueue
			uniqueResults, err := c.broker.EnqueueUniqueBatch(ctx, regularMsgs, taskOpts[0].uniqueTTL)
			if err != nil {
				enqueueErrs = append(enqueueErrs, fmt.Errorf("failed to enqueue unique batch: %v", err))
			} else {
				for i, n := range uniqueResults {
					msg := regularMsgs[i]
					switch n {
					case 1: // Success
						results = append(results, newTaskInfo(msg, base.TaskStatePending, now, nil))
					case 0: // ID conflict
						enqueueErrs = append(enqueueErrs, fmt.Errorf("task %s: %w", msg.ID, ErrTaskIDConflict))
					case -1: // Unique key conflict
						enqueueErrs = append(enqueueErrs, fmt.Errorf("task %s: %w", msg.ID, ErrDuplicateTask))
					}
				}
			}
		} else {
			// Use regular batch enqueue
			batchResults, err := c.broker.EnqueueBatch(ctx, regularMsgs)
			if err != nil {
				enqueueErrs = append(enqueueErrs, fmt.Errorf("failed to enqueue batch: %v", err))
			} else {
				for i, n := range batchResults {
					msg := regularMsgs[i]
					switch n {
					case 1: // Success
						results = append(results, newTaskInfo(msg, base.TaskStatePending, now, nil))
					case 0: // ID conflict
						enqueueErrs = append(enqueueErrs, fmt.Errorf("task %s: %w", msg.ID, ErrTaskIDConflict))
					}
				}
			}
		}
	}

	// Process scheduled messages
	for _, st := range scheduledMsgs {
		var err error
		if st.msg.UniqueKey != "" {
			ttl := time.Until(st.processAt.Add(taskOpts[0].uniqueTTL))
			err = c.broker.ScheduleUnique(ctx, st.msg, st.processAt, ttl)
		} else {
			err = c.broker.Schedule(ctx, st.msg, st.processAt)
		}

		if err != nil {
			enqueueErrs = append(enqueueErrs, fmt.Errorf("failed to schedule task %s: %v", st.msg.ID, err))
		} else {
			results = append(results, newTaskInfo(st.msg, base.TaskStateScheduled, st.processAt, nil))
		}
	}

	// Process group messages
	for _, msg := range groupMsgs {
		var err error
		if msg.UniqueKey != "" {
			err = c.broker.AddToGroupUnique(ctx, msg, msg.GroupKey, taskOpts[0].uniqueTTL)
		} else {
			err = c.broker.AddToGroup(ctx, msg, msg.GroupKey)
		}

		if err != nil {
			enqueueErrs = append(enqueueErrs, fmt.Errorf("failed to add task %s to group: %v", msg.ID, err))
		} else {
			results = append(results, newTaskInfo(msg, base.TaskStateAggregating, time.Time{}, nil))
		}
	}

	// Sort results to match the input order
	orderedResults := make([]*TaskInfo, len(tasks))
	resultMap := make(map[string]*TaskInfo)
	for _, res := range results {
		resultMap[res.ID] = res
	}

	// Create a map of task message IDs by their index
	taskMsgIDs := make(map[int]string, len(msgs))
	for i, msg := range msgs {
		taskMsgIDs[i] = msg.ID
	}

	// Match results with original task order
	for i := range tasks {
		if msgID, ok := taskMsgIDs[i]; ok {
			if res, ok := resultMap[msgID]; ok {
				orderedResults[i] = res
			}
		}
	}

	// If there were any errors, return them along with the partial results
	if len(enqueueErrs) > 0 {
		var errMsgs []string
		for _, err := range enqueueErrs {
			errMsgs = append(errMsgs, err.Error())
		}
		return orderedResults, fmt.Errorf("batch enqueue completed with %d errors: %s",
			len(enqueueErrs), strings.Join(errMsgs, "; "))
	}

	return orderedResults, nil
}

// scheduledTask represents a task that is scheduled to be processed in the future.
type scheduledTask struct {
	msg       *base.TaskMessage
	processAt time.Time
}

// Ping performs a ping against the redis connection.
func (c *Client) Ping() error {
	return c.broker.Ping()
}

func (c *Client) enqueue(ctx context.Context, msg *base.TaskMessage, uniqueTTL time.Duration) error {
	if uniqueTTL > 0 {
		return c.broker.EnqueueUnique(ctx, msg, uniqueTTL)
	}
	return c.broker.Enqueue(ctx, msg)
}

func (c *Client) schedule(ctx context.Context, msg *base.TaskMessage, t time.Time, uniqueTTL time.Duration) error {
	if uniqueTTL > 0 {
		ttl := time.Until(t.Add(uniqueTTL))
		return c.broker.ScheduleUnique(ctx, msg, t, ttl)
	}
	return c.broker.Schedule(ctx, msg, t)
}

func (c *Client) addToGroup(ctx context.Context, msg *base.TaskMessage, group string, uniqueTTL time.Duration) error {
	if uniqueTTL > 0 {
		return c.broker.AddToGroupUnique(ctx, msg, group, uniqueTTL)
	}
	return c.broker.AddToGroup(ctx, msg, group)
}

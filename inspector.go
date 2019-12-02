package asynq

// Inspector is used to inspect queues.
type Inspector struct {
	rdb *rdb
}

// NewInspector returns a new Inspector instance.
func NewInspector(opt *RedisOpt) *Inspector {
	return &Inspector{
		rdb: newRDB(opt),
	}
}

// CurrentStats returns a current stats of queues.
func (i *Inspector) CurrentStats() (*Stats, error) {
	return i.rdb.currentStats()
}

// toTaskSlice converts a taskMessage slice to a Task slice.
func (i *Inspector) toTaskSlice(msgs []*taskMessage) []*Task {
	var tasks []*Task
	for _, m := range msgs {
		tasks = append(tasks, &Task{Type: m.Type, Payload: m.Payload})
	}
	return tasks
}

// ListEnqueuedTasks returns a list of tasks ready to be processed.
func (i *Inspector) ListEnqueuedTasks() ([]*EnqueuedTask, error) {
	// TODO(hibiken): Support pagination.
	msgs, err := i.rdb.listEnqueued()
	if err != nil {
		return nil, err
	}
	var tasks []*EnqueuedTask
	for _, m := range msgs {
		tasks = append(tasks, &EnqueuedTask{
			ID:      m.ID,
			Type:    m.Type,
			Payload: m.Payload,
		})
	}
	return tasks, nil
}

// ListInProgressTasks returns a list of tasks that are being processed.
func (i *Inspector) ListInProgressTasks() ([]*InProgressTask, error) {
	// TODO(hibiken): Support pagination.
	msgs, err := i.rdb.listInProgress()
	if err != nil {
		return nil, err
	}
	var tasks []*InProgressTask
	for _, m := range msgs {
		tasks = append(tasks, &InProgressTask{
			ID:      m.ID,
			Type:    m.Type,
			Payload: m.Payload,
		})
	}
	return tasks, nil
}

// ListScheduledTasks returns a list of tasks that are scheduled to
// be processed in the future.
func (i *Inspector) ListScheduledTasks() ([]*ScheduledTask, error) {
	// TODO(hibiken): Support pagination.
	return i.rdb.listScheduled()
}

// ListRetryTasks returns a list of tasks what will be retried in the
// future.
func (i *Inspector) ListRetryTasks() ([]*RetryTask, error) {
	// TODO(hibiken): Support pagination.
	return i.rdb.listRetry()
}

// ListDeadTasks returns a list of tasks that have reached its
// maximum retry limit.
func (i *Inspector) ListDeadTasks() ([]*DeadTask, error) {
	// TODO(hibiken): Support pagination.
	return i.rdb.listDead()
}

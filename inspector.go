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
func (i *Inspector) ListEnqueuedTasks() ([]*Task, error) {
	// TODO(hibiken): Support pagination.
	msgs, err := i.rdb.listEnqueued()
	if err != nil {
		return nil, err
	}
	return i.toTaskSlice(msgs), nil
}

// ListInProgressTasks returns a list of tasks that are being processed.
func (i *Inspector) ListInProgressTasks() ([]*Task, error) {
	// TODO(hibiken): Support pagination.
	msgs, err := i.rdb.listInProgress()
	if err != nil {
		return nil, err
	}
	return i.toTaskSlice(msgs), nil
}

// ListScheduledTasks returns a list of tasks that are scheduled to
// be processed in the future.
func (i *Inspector) ListScheduledTasks() ([]*Task, error) {
	// TODO(hibiken): Support pagination.
	msgs, err := i.rdb.listScheduled()
	if err != nil {
		return nil, err
	}
	return i.toTaskSlice(msgs), nil
}

// ListRetryTasks returns a list of tasks what will be retried in the
// future.
func (i *Inspector) ListRetryTasks() ([]*Task, error) {
	// TODO(hibiken): Support pagination.
	msgs, err := i.rdb.listRetry()
	if err != nil {
		return nil, err
	}
	return i.toTaskSlice(msgs), nil
}

// ListDeadTasks returns a list of tasks that have reached its
// maximum retry limit.
func (i *Inspector) ListDeadTasks() ([]*Task, error) {
	// TODO(hibiken): Support pagination.
	msgs, err := i.rdb.listDead()
	if err != nil {
		return nil, err
	}
	return i.toTaskSlice(msgs), nil
}

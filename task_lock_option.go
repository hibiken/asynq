package asynq

const taskLockHeader = "asynq_lock"
const taskLockKeyHeader = "asynq_lock_key"

// TaskLockHeader is the internal task header used to signal that a task should
// acquire a distributed processing lock before the handler runs.
const TaskLockHeader = taskLockHeader

// TaskLockKeyHeader is the internal task header used to store a custom
// distributed lock key for the task.
const TaskLockKeyHeader = taskLockKeyHeader

func taskLockHeaders(taskHeaders map[string]string, enabled bool, lockKey string) map[string]string {
	if len(taskHeaders) == 0 && !enabled {
		return taskHeaders
	}

	headers := make(map[string]string, len(taskHeaders)+2)
	for k, v := range taskHeaders {
		headers[k] = v
	}
	if !enabled {
		return headers
	}
	headers[taskLockHeader] = "1"
	if lockKey != "" {
		headers[taskLockKeyHeader] = lockKey
	}
	return headers
}

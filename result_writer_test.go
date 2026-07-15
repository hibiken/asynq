package asynq

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/rdb"
)

func TestResultWriterWriteRejectsMissingTask(t *testing.T) {
	redisClient := setup(t)
	defer redisClient.Close()

	const queue = "default"
	taskID := uuid.NewString()
	if err := redisClient.SAdd(context.Background(), base.AllQueues, queue).Err(); err != nil {
		t.Fatalf("failed to register queue: %v", err)
	}
	writer := &ResultWriter{
		id:     taskID,
		qname:  queue,
		broker: rdb.NewRDB(redisClient),
		ctx:    context.Background(),
	}

	n, err := writer.Write([]byte("result"))

	if !errors.Is(err, ErrTaskNotFound) {
		t.Errorf("ResultWriter.Write error = %v, want ErrTaskNotFound", err)
	}
	if n != 0 {
		t.Errorf("ResultWriter.Write returned %d, want 0", n)
	}
	if got := redisClient.Exists(context.Background(), base.TaskKey(queue, taskID)).Val(); got != 0 {
		t.Errorf("missing task key exists after failed result write: got %d, want 0", got)
	}

	inspector := NewInspectorFromRedisClient(redisClient)
	if _, err := inspector.GetTaskInfo(queue, taskID); !errors.Is(err, ErrTaskNotFound) {
		t.Errorf("Inspector.GetTaskInfo error = %v, want ErrTaskNotFound", err)
	}
	if err := inspector.DeleteTask(queue, taskID); !errors.Is(err, ErrTaskNotFound) {
		t.Errorf("Inspector.DeleteTask error = %v, want ErrTaskNotFound", err)
	}

	client := NewClientFromRedisClient(redisClient)
	if _, err := client.Enqueue(
		NewTask("task", nil),
		Queue(queue),
		TaskID(taskID),
		Unique(time.Minute),
	); err != nil {
		t.Errorf("Client.Enqueue with TaskID and Unique failed: %v", err)
	}
}

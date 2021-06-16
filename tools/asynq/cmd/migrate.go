// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/google/uuid"
	"github.com/hibiken/asynq/internal/base"
	"github.com/spf13/cobra"
)

// migrateCmd represents the migrate command.
var migrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: fmt.Sprintf("Migrate existing tasks and queues to be asynq%s compatible", base.Version),
	Long: `Migrate (asynq migrate) will migrate existing tasks and queues in redis to be compatible with the latest version of asynq.
`,
	Args: cobra.NoArgs,
	Run:  migrate,
}

func init() {
	rootCmd.AddCommand(migrateCmd)
}

// Migration Steps:
//
// Step 1: Backup your Redis DB with RDB snapshot file
//
// Step 2: Run the following command to update the DB with new schema:
// asynq migrate
//
// Step 3 (Optional):
// If Step 2 fails, restore from the backup and report an issue.

func backupKey(key string) string {
	return fmt.Sprintf("%s:backup", key)
}

func renameKeyAsBackup(c redis.UniversalClient, key string) error {
	if c.Exists(key).Val() == 0 {
		return nil // key doesn't exist; no-op
	}
	return c.Rename(key, backupKey(key)).Err()
}

func failIfError(err error, msg string) {
	if err != nil {
		fmt.Printf("error: %s: %v\n", msg, err)
		fmt.Println("*** Please report this issue at https://github.com/hibiken/asynq/issues ***")
		os.Exit(1)
	}
}

func logIfError(err error, msg string) {
	if err != nil {
		fmt.Printf("warning: %s: %v\n", msg, err)
	}
}

func migrate(cmd *cobra.Command, args []string) {
	r := createRDB()

	// Rename key asynq:{<qname>} -> asynq:{<qname>}:pending
	queues, err := r.AllQueues()
	failIfError(err, "Failed to get queue names")

	fmt.Print("Renaming pending keys...")
	for _, qname := range queues {
		oldKey := fmt.Sprintf("asynq:{%s}", qname)
		if r.Client().Exists(oldKey).Val() == 0 {
			continue
		}
		newKey := base.PendingKey(qname)
		err := r.Client().Rename(oldKey, newKey).Err()
		failIfError(err, "Failed to rename key")
	}
	fmt.Print("Done\n")

	// Rename LIST/ZSET keys as backup
	fmt.Print("Renaming keys for backup...")
	for _, qname := range queues {
		keys := []string{
			base.ActiveKey(qname),
			base.PendingKey(qname),
			base.ScheduledKey(qname),
			base.RetryKey(qname),
			base.ArchivedKey(qname),
		}
		for _, key := range keys {
			err := renameKeyAsBackup(r.Client(), key)
			failIfError(err, fmt.Sprintf("Failed to rename key %q for backup", key))
		}
	}
	fmt.Print("Done\n")

	fmt.Print("Updating to new schema...")
	// All list/zset should contain task-ids and store task data under task key
	// - Unmarshal using old task schema
	// - Marshal data using new schema

	for _, qname := range queues {
		// Active Tasks + Deadlines set

		// Pending Tasks
		data, err := r.Client().LRange(backupKey(base.PendingKey(qname)), 0, -1).Result()
		failIfError(err, "Failed to read backup pending key")

		for _, s := range data {
			msg, err := UnmarshalOldMessage(s)
			failIfError(err, "Failed to unmarshal message")

			if msg.UniqueKey != "" {
				ttl, err := r.Client().TTL(msg.UniqueKey).Result()
				failIfError(err, "Failed to get ttl")

				if ttl > 0 {
					err = r.Client().Del(msg.UniqueKey).Err()
					logIfError(err, "Failed to delete unique key")
				}

				// Regenerate unique key.
				msg.UniqueKey = base.UniqueKey(msg.Queue, msg.Type, msg.Payload)
				if ttl > 0 {
					err = r.EnqueueUnique(msg, ttl)
				} else {
					err = r.Enqueue(msg)
				}
				failIfError(err, "Failed to enqueue pending message")

			} else {
				err := r.Enqueue(msg)
				failIfError(err, "Failed to enqueue pending message")
			}
		}

		// Scheduled Tasks
		zs, err := r.Client().ZRangeWithScores(backupKey(base.ScheduledKey(qname)), 0, -1).Result()
		failIfError(err, "Failed to read")

		for _, z := range zs {
			msg, err := UnmarshalOldMessage(z.Member.(string))
			failIfError(err, "Failed to unmarshal message")

			processAt := time.Unix(int64(z.Score), 0)

			if msg.UniqueKey != "" {
				ttl, err := r.Client().TTL(msg.UniqueKey).Result()
				failIfError(err, "Failed to get ttl")

				if ttl > 0 {
					err = r.Client().Del(msg.UniqueKey).Err()
					logIfError(err, "Failed to delete unique key")
				}

				// Regenerate unique key.
				msg.UniqueKey = base.UniqueKey(msg.Queue, msg.Type, msg.Payload)
				if ttl > 0 {
					err = r.ScheduleUnique(msg, processAt, ttl)
				} else {
					err = r.Schedule(msg, processAt)
				}
				failIfError(err, "Failed to enqueue pending message")
			} else {
				err := r.Schedule(msg, processAt)
				failIfError(err, "Failed to enqueue scheduled message")
			}
		}

		// Retry Tasks

		// Archived Tasks

	}
	fmt.Print("Done\n")

	fmt.Print("Deleting backup keys...")
	for _, qname := range queues {
		keys := []string{
			backupKey(base.ActiveKey(qname)),
			backupKey(base.PendingKey(qname)),
			backupKey(base.ScheduledKey(qname)),
			backupKey(base.RetryKey(qname)),
			backupKey(base.ArchivedKey(qname)),
		}
		for _, key := range keys {
			err := r.Client().Del(key).Err()
			failIfError(err, "Failed to delete backup key")
		}
	}
	fmt.Print("Done\n")
}

func UnmarshalOldMessage(encoded string) (*base.TaskMessage, error) {
	oldMsg, err := DecodeMessage(encoded)
	if err != nil {
		return nil, err
	}
	payload, err := json.Marshal(oldMsg.Payload)
	if err != nil {
		return nil, fmt.Errorf("could not marshal payload: %v", err)
	}
	return &base.TaskMessage{
		Type:         oldMsg.Type,
		Payload:      payload,
		ID:           oldMsg.ID,
		Queue:        oldMsg.Queue,
		Retry:        oldMsg.Retry,
		Retried:      oldMsg.Retried,
		ErrorMsg:     oldMsg.ErrorMsg,
		LastFailedAt: 0,
		Timeout:      oldMsg.Timeout,
		Deadline:     oldMsg.Deadline,
		UniqueKey:    oldMsg.UniqueKey,
	}, nil
}

// TaskMessage from v0.17
type OldTaskMessage struct {
	// Type indicates the kind of the task to be performed.
	Type string

	// Payload holds data needed to process the task.
	Payload map[string]interface{}

	// ID is a unique identifier for each task.
	ID uuid.UUID

	// Queue is a name this message should be enqueued to.
	Queue string

	// Retry is the max number of retry for this task.
	Retry int

	// Retried is the number of times we've retried this task so far.
	Retried int

	// ErrorMsg holds the error message from the last failure.
	ErrorMsg string

	// Timeout specifies timeout in seconds.
	// If task processing doesn't complete within the timeout, the task will be retried
	// if retry count is remaining. Otherwise it will be moved to the archive.
	//
	// Use zero to indicate no timeout.
	Timeout int64

	// Deadline specifies the deadline for the task in Unix time,
	// the number of seconds elapsed since January 1, 1970 UTC.
	// If task processing doesn't complete before the deadline, the task will be retried
	// if retry count is remaining. Otherwise it will be moved to the archive.
	//
	// Use zero to indicate no deadline.
	Deadline int64

	// UniqueKey holds the redis key used for uniqueness lock for this task.
	//
	// Empty string indicates that no uniqueness lock was used.
	UniqueKey string
}

// DecodeMessage unmarshals the given encoded string and returns a decoded task message.
// Code from v0.17.
func DecodeMessage(s string) (*OldTaskMessage, error) {
	d := json.NewDecoder(strings.NewReader(s))
	d.UseNumber()
	var msg OldTaskMessage
	if err := d.Decode(&msg); err != nil {
		return nil, err
	}
	return &msg, nil
}

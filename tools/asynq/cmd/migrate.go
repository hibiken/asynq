// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

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

func migrate(cmd *cobra.Command, args []string) {
	r := createRDB()

	// Rename key asynq:{<qname>} -> asynq:{<qname>}:pending
	queues, err := r.AllQueues()
	if err != nil {
		fmt.Printf("(error): Failed to get queue names: %v", err)
		os.Exit(1)
	}
	fmt.Print("Renaming pending keys...")
	for _, qname := range queues {
		oldKey := fmt.Sprintf("asynq:{%s}", qname)
		if r.Client().Exists(oldKey).Val() == 0 {
			continue
		}
		newKey := base.PendingKey(qname)
		if err := r.Client().Rename(oldKey, newKey).Err(); err != nil {
			fmt.Printf("(error): Failed to rename key: %v", err)
			os.Exit(1)
		}
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
			if err := renameKeyAsBackup(r.Client(), key); err != nil {
				fmt.Printf("(error): Failed to rename key %q for backup: %v", key, err)
				os.Exit(1)
			}
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
		if err != nil {
			fmt.Printf("(error): Failed to read: %v", err)
			os.Exit(1)
		}
		for _, s := range data {
			msg, err := UnmarshalOldMessage(s)
			if err != nil {
				fmt.Printf("(error): Failed to unmarshal message: %v", err)
				os.Exit(1)
			}
			if msg.UniqueKey != "" {
				ttl, err := r.Client().TTL(msg.UniqueKey).Result()
				if err != nil {
					fmt.Printf("(error): Failed to get ttl: %v", err)
					os.Exit(1)
				}
				if err := r.Client().Del(msg.UniqueKey).Err(); err != nil {
					fmt.Printf("(error): Failed to delete unique key")
					os.Exit(1)
				}
				// Regenerate unique key.
				msg.UniqueKey = base.UniqueKey(msg.Queue, msg.Type, msg.Payload)
				if err := r.EnqueueUnique(msg, ttl); err != nil {
					fmt.Printf("(error): Failed to enqueue unique pending message: %v", err)
					os.Exit(1)
				}

			} else {
				if err := r.Enqueue(msg); err != nil {
					fmt.Printf("(error): Failed to enqueue pending message: %v", err)
					os.Exit(1)
				}
			}
		}

		// Scheduled Tasks
		// data, err = r.Client().ZRangeWithScores(backupKey(base.ScheduledKey(qname)), 0, -1).Result()
		// if err != nil {
		// 	fmt.Printf("(error): Failed to read: %v", err)
		// 	os.Exit(1)
		// }
		// for _, z := range data {
		// 	msg, err := UnmarsalOldMessage(z.Member.(string))
		// 	if err != nil {
		// 		fmt.Printf("(error): Failed to unmarshal message: %v", err)
		// 		os.Exit(1)
		// 	}
		// 	task := asynq.NewTask(msg.Type, msg.Payload)
		// 	opts := createOptions(msg, r.Client())
		// 	opts = append(opts, asynq.ProcessAt(time.Unix(z.Score, 0)))
		// 	if _, err := c.Enqueue(task, opts...); err != nil {
		// 		fmt.Printf("(error): Failed to enqueue task: %v", err)
		// 		os.Exit(1)
		// 	}
		// }

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
			if err := r.Client().Del(key).Err(); err != nil {
				fmt.Printf("(error): Failed to delete backup key: %v", err)
				os.Exit(1)
			}
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

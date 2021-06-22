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
	"github.com/hibiken/asynq/internal/errors"
	"github.com/hibiken/asynq/internal/rdb"
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
	queues, err := r.AllQueues()
	failIfError(err, "Failed to get queue names")

	// ---------------------------------------------
	// Pre-check: Ensure no active servers, tasks.
	// ---------------------------------------------
	srvs, err := r.ListServers()
	failIfError(err, "Failed to get server infos")
	if len(srvs) > 0 {
		fmt.Println("(error): Server(s) still running. Please ensure that no asynq servers are running when runnning migrate command.")
		os.Exit(1)
	}
	for _, qname := range queues {
		stats, err := r.CurrentStats(qname)
		failIfError(err, "Failed to get stats")
		if stats.Active > 0 {
			fmt.Printf("(error): %d active tasks found. Please ensure that no active tasks exist when running migrate command.\n", stats.Active)
			os.Exit(1)
		}
	}

	// ---------------------------------------------
	// Rename pending key
	// ---------------------------------------------
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

	// ---------------------------------------------
	// Rename keys as backup
	// ---------------------------------------------
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

	// ---------------------------------------------
	// Update to new schema
	// ---------------------------------------------
	fmt.Print("Updating to new schema...")
	for _, qname := range queues {
		updatePendingMessages(r, qname)
		updateZSetMessages(r.Client(), base.ScheduledKey(qname), "scheduled")
		updateZSetMessages(r.Client(), base.RetryKey(qname), "retry")
		updateZSetMessages(r.Client(), base.ArchivedKey(qname), "archived")
	}
	fmt.Print("Done\n")

	// ---------------------------------------------
	// Delete backup keys
	// ---------------------------------------------
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

func updatePendingMessages(r *rdb.RDB, qname string) {
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
			failIfError(err, "Failed to enqueue message")

		} else {
			err := r.Enqueue(msg)
			failIfError(err, "Failed to enqueue message")
		}
	}
}

// KEYS[1] -> asynq:{<qname>}:t:<task_id>
// KEYS[2] -> asynq:{<qname>}:scheduled
// ARGV[1] -> task message data
// ARGV[2] -> zset score
// ARGV[3] -> task ID
// ARGV[4] -> task timeout in seconds (0 if not timeout)
// ARGV[5] -> task deadline in unix time (0 if no deadline)
// ARGV[6] -> task state (e.g. "retry", "archived")
var taskZAddCmd = redis.NewScript(`
redis.call("HSET", KEYS[1],
           "msg", ARGV[1],
           "state", ARGV[6],
           "timeout", ARGV[4],
           "deadline", ARGV[5])
redis.call("ZADD", KEYS[2], ARGV[2], ARGV[3])
return 1
`)

// ZAddTask adds task to zset.
func ZAddTask(c redis.UniversalClient, key string, msg *base.TaskMessage, score float64, state string) error {
	// Special case; LastFailedAt field is new so assign a value inferred from zscore.
	if state == "archived" {
		msg.LastFailedAt = int64(score)
	}

	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		return err
	}
	if err := c.SAdd(base.AllQueues, msg.Queue).Err(); err != nil {
		return err
	}
	keys := []string{
		base.TaskKey(msg.Queue, msg.ID.String()),
		key,
	}
	argv := []interface{}{
		encoded,
		score,
		msg.ID.String(),
		msg.Timeout,
		msg.Deadline,
		state,
	}
	return taskZAddCmd.Run(c, keys, argv...).Err()
}

// KEYS[1] -> unique key
// KEYS[2] -> asynq:{<qname>}:t:<task_id>
// KEYS[3] -> zset key (e.g. asynq:{<qname>}:scheduled)
// --
// ARGV[1] -> task ID
// ARGV[2] -> uniqueness lock TTL
// ARGV[3] -> score (process_at timestamp)
// ARGV[4] -> task message
// ARGV[5] -> task timeout in seconds (0 if not timeout)
// ARGV[6] -> task deadline in unix time (0 if no deadline)
// ARGV[7] -> task state (oneof "scheduled", "retry", "archived")
var taskZAddUniqueCmd = redis.NewScript(`
local ok = redis.call("SET", KEYS[1], ARGV[1], "NX", "EX", ARGV[2])
if not ok then
  return 0
end
redis.call("HSET", KEYS[2],
           "msg", ARGV[4],
           "state", ARGV[7],
           "timeout", ARGV[5],
           "deadline", ARGV[6],
           "unique_key", KEYS[1])
redis.call("ZADD", KEYS[3], ARGV[3], ARGV[1])
return 1
`)

// ScheduleUnique adds the task to the backlog queue to be processed in the future if the uniqueness lock can be acquired.
// It returns ErrDuplicateTask if the lock cannot be acquired.
func ZAddTaskUnique(c redis.UniversalClient, key string, msg *base.TaskMessage, score float64, state string, ttl time.Duration) error {
	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		return err
	}
	if err := c.SAdd(base.AllQueues, msg.Queue).Err(); err != nil {
		return err
	}
	keys := []string{
		msg.UniqueKey,
		base.TaskKey(msg.Queue, msg.ID.String()),
		key,
	}
	argv := []interface{}{
		msg.ID.String(),
		int(ttl.Seconds()),
		score,
		encoded,
		msg.Timeout,
		msg.Deadline,
		state,
	}
	res, err := taskZAddUniqueCmd.Run(c, keys, argv...).Result()
	if err != nil {
		return err
	}
	n, ok := res.(int64)
	if !ok {
		return errors.E(errors.Internal, fmt.Sprintf("cast error: unexpected return value from Lua script: %v", res))
	}
	if n == 0 {
		return errors.E(errors.AlreadyExists, errors.ErrDuplicateTask)
	}
	return nil
}

func updateZSetMessages(c redis.UniversalClient, key, state string) {
	zs, err := c.ZRangeWithScores(backupKey(key), 0, -1).Result()
	failIfError(err, "Failed to read")

	for _, z := range zs {
		msg, err := UnmarshalOldMessage(z.Member.(string))
		failIfError(err, "Failed to unmarshal message")

		if msg.UniqueKey != "" {
			ttl, err := c.TTL(msg.UniqueKey).Result()
			failIfError(err, "Failed to get ttl")

			if ttl > 0 {
				err = c.Del(msg.UniqueKey).Err()
				logIfError(err, "Failed to delete unique key")
			}

			// Regenerate unique key.
			msg.UniqueKey = base.UniqueKey(msg.Queue, msg.Type, msg.Payload)
			if ttl > 0 {
				err = ZAddTaskUnique(c, key, msg, z.Score, state, ttl)
			} else {
				err = ZAddTask(c, key, msg, z.Score, state)
			}
			failIfError(err, "Failed to zadd message")
		} else {
			err := ZAddTask(c, key, msg, z.Score, state)
			failIfError(err, "Failed to enqueue scheduled message")
		}
	}
}

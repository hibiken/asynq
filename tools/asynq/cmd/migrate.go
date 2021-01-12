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
	"github.com/spf13/cast"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var migrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: fmt.Sprintf("Migrate all tasks to be compatible with asynq v%s", base.Version),
	Args:  cobra.NoArgs,
	Run:   migrate,
}

func init() {
	rootCmd.AddCommand(migrateCmd)
}

func migrate(cmd *cobra.Command, args []string) {
	c := redis.NewClient(&redis.Options{
		Addr:     viper.GetString("uri"),
		DB:       viper.GetInt("db"),
		Password: viper.GetString("password"),
	})
	r := createRDB()

	/*** Migrate from 0.9 to 0.10, 0.11 compatible ***/
	lists := []string{"asynq:in_progress"}
	allQueues, err := c.SMembers(base.AllQueues).Result()
	if err != nil {
		printError(fmt.Errorf("could not read all queues: %v", err))
		os.Exit(1)
	}
	lists = append(lists, allQueues...)
	for _, key := range lists {
		if err := migrateList(c, key); err != nil {
			printError(err)
			os.Exit(1)
		}
	}

	zsets := []string{"asynq:scheduled", "asynq:retry", "asynq:dead"}
	for _, key := range zsets {
		if err := migrateZSet(c, key); err != nil {
			printError(err)
			os.Exit(1)
		}
	}

	/*** Migrate from 0.11 to 0.12 compatible ***/
	if err := createBackup(c, base.AllQueues); err != nil {
		printError(err)
		os.Exit(1)
	}
	for _, qkey := range allQueues {
		qname := strings.TrimPrefix(qkey, "asynq:queues:")
		if err := c.SAdd(base.AllQueues, qname).Err(); err != nil {
			err = fmt.Errorf("could not add queue name %q to %q set: %v\n",
				qname, base.AllQueues, err)
			printError(err)
			os.Exit(1)
		}
	}
	if err := deleteBackup(c, base.AllQueues); err != nil {
		printError(err)
		os.Exit(1)
	}

	for _, qkey := range allQueues {
		qname := strings.TrimPrefix(qkey, "asynq:queues:")
		if exists := c.Exists(qkey).Val(); exists == 1 {
			if err := c.Rename(qkey, base.QueueKey(qname)).Err(); err != nil {
				printError(fmt.Errorf("could not rename key %q: %v\n", qkey, err))
				os.Exit(1)
			}
		}
	}

	if err := partitionZSetMembersByQueue(c, "asynq:scheduled", base.ScheduledKey); err != nil {
		printError(err)
		os.Exit(1)
	}
	if err := partitionZSetMembersByQueue(c, "asynq:retry", base.RetryKey); err != nil {
		printError(err)
		os.Exit(1)
	}
	// Note: base.DeadKey function was renamed in v0.14. We define the legacy function here since we need it for this migration script.
	deadKeyFunc := func(qname string) string { return fmt.Sprintf("asynq:{%s}:dead", qname) } 
	if err := partitionZSetMembersByQueue(c, "asynq:dead", deadKeyFunc); err != nil {
		printError(err)
		os.Exit(1)
	}
	if err := partitionZSetMembersByQueue(c, "asynq:deadlines", base.DeadlinesKey); err != nil {
		printError(err)
		os.Exit(1)
	}
	if err := partitionListMembersByQueue(c, "asynq:in_progress", base.ActiveKey); err != nil {
		printError(err)
		os.Exit(1)
	}

	paused, err := c.SMembers("asynq:paused").Result()
	if err != nil {
		printError(fmt.Errorf("command SMEMBERS asynq:paused failed: %v", err))
		os.Exit(1)
	}
	for _, qkey := range paused {
		qname := strings.TrimPrefix(qkey, "asynq:queues:")
		if err := r.Pause(qname); err != nil {
			printError(err)
			os.Exit(1)
		}
	}
	if err := deleteKey(c, "asynq:paused"); err != nil {
		printError(err)
		os.Exit(1)
	}

	if err := deleteKey(c, "asynq:servers"); err != nil {
		printError(err)
		os.Exit(1)
	}
	if err := deleteKey(c, "asynq:workers"); err != nil {
		printError(err)
		os.Exit(1)
	}

	/*** Migrate from 0.13 to 0.14 compatible ***/

	// Move all dead tasks to archived ZSET.
	for _, qname := range allQueues {
		zs, err := c.ZRangeWithScores(deadKeyFunc(qname), 0, -1).Result()
		if err != nil {
			printError(err)
			os.Exit(1)
		}
		for _, z := range zs {
			if err := c.ZAdd(base.ArchivedKey(qname), &z).Err(); err != nil {
				printError(err)
				os.Exit(1)
			}
		}
		if err := deleteKey(c, deadKeyFunc(qname)); err != nil {
			printError(err)
			os.Exit(1)
		}
	}
}

func backupKey(key string) string {
	return fmt.Sprintf("%s:backup", key)
}

func createBackup(c *redis.Client, key string) error {
	err := c.Rename(key, backupKey(key)).Err()
	if err != nil {
		return fmt.Errorf("could not rename key %q: %v", key, err)
	}
	return nil
}

func deleteBackup(c *redis.Client, key string) error {
	return deleteKey(c, backupKey(key))
}

func deleteKey(c *redis.Client, key string) error {
	exists := c.Exists(key).Val()
	if exists == 0 {
		// key does not exist
		return nil
	}
	err := c.Del(key).Err()
	if err != nil {
		return fmt.Errorf("could not delete key %q: %v", key, err)
	}
	return nil
}

func printError(err error) {
	fmt.Println(err)
	fmt.Println()
	fmt.Println("Migrate command error")
	fmt.Println("Please file an issue on Github at https://github.com/hibiken/asynq/issues/new/choose")
}

func partitionZSetMembersByQueue(c *redis.Client, key string, newKeyFunc func(string) string) error {
	zs, err := c.ZRangeWithScores(key, 0, -1).Result()
	if err != nil {
		return fmt.Errorf("command ZRANGE %s 0 -1 WITHSCORES failed: %v", key, err)
	}
	for _, z := range zs {
		s := cast.ToString(z.Member)
		msg, err := base.DecodeMessage(s)
		if err != nil {
			return fmt.Errorf("could not decode message from %q: %v", key, err)
		}
		if err := c.ZAdd(newKeyFunc(msg.Queue), &z).Err(); err != nil {
			return fmt.Errorf("could not add %v to %q: %v", z, newKeyFunc(msg.Queue))
		}
	}
	if err := deleteKey(c, key); err != nil {
		return err
	}
	return nil
}

func partitionListMembersByQueue(c *redis.Client, key string, newKeyFunc func(string) string) error {
	data, err := c.LRange(key, 0, -1).Result()
	if err != nil {
		return fmt.Errorf("command LRANGE %s 0 -1 failed: %v", key, err)
	}
	for _, s := range data {
		msg, err := base.DecodeMessage(s)
		if err != nil {
			return fmt.Errorf("could not decode message from %q: %v", key, err)
		}
		if err := c.LPush(newKeyFunc(msg.Queue), s).Err(); err != nil {
			return fmt.Errorf("could not add %v to %q: %v", s, newKeyFunc(msg.Queue))
		}
	}
	if err := deleteKey(c, key); err != nil {
		return err
	}
	return nil
}

type oldTaskMessage struct {
	// Unchanged
	Type      string
	Payload   map[string]interface{}
	ID        uuid.UUID
	Queue     string
	Retry     int
	Retried   int
	ErrorMsg  string
	UniqueKey string

	// Following fields have changed.

	// Deadline specifies the deadline for the task.
	// Task won't be processed if it exceeded its deadline.
	// The string shoulbe be in RFC3339 format.
	//
	// time.Time's zero value means no deadline.
	Timeout string

	// Deadline specifies the deadline for the task.
	// Task won't be processed if it exceeded its deadline.
	// The string shoulbe be in RFC3339 format.
	//
	// time.Time's zero value means no deadline.
	Deadline string
}

var defaultTimeout = 30 * time.Minute

func convertMessage(old *oldTaskMessage) (*base.TaskMessage, error) {
	timeout, err := time.ParseDuration(old.Timeout)
	if err != nil {
		return nil, fmt.Errorf("could not parse Timeout field of %+v", old)
	}
	deadline, err := time.Parse(time.RFC3339, old.Deadline)
	if err != nil {
		return nil, fmt.Errorf("could not parse Deadline field of %+v", old)
	}
	if timeout == 0 && deadline.IsZero() {
		timeout = defaultTimeout
	}
	if deadline.IsZero() {
		// Zero value used to be time.Time{},
		// in the new schema zero value is represented by
		// zero in Unix time.
		deadline = time.Unix(0, 0)
	}
	return &base.TaskMessage{
		Type:      old.Type,
		Payload:   old.Payload,
		ID:        uuid.New(),
		Queue:     old.Queue,
		Retry:     old.Retry,
		Retried:   old.Retried,
		ErrorMsg:  old.ErrorMsg,
		UniqueKey: old.UniqueKey,
		Timeout:   int64(timeout.Seconds()),
		Deadline:  deadline.Unix(),
	}, nil
}

func deserialize(s string) (*base.TaskMessage, error) {
	// Try deserializing as old message.
	d := json.NewDecoder(strings.NewReader(s))
	d.UseNumber()
	var old *oldTaskMessage
	if err := d.Decode(&old); err != nil {
		// Try deserializing as new message.
		d = json.NewDecoder(strings.NewReader(s))
		d.UseNumber()
		var msg *base.TaskMessage
		if err := d.Decode(&msg); err != nil {
			return nil, fmt.Errorf("could not deserialize %s into task message: %v", s, err)
		}
		return msg, nil
	}
	return convertMessage(old)
}

func migrateZSet(c *redis.Client, key string) error {
	if c.Exists(key).Val() == 0 {
		// skip if key doesn't exist.
		return nil
	}
	res, err := c.ZRangeWithScores(key, 0, -1).Result()
	if err != nil {
		return err
	}
	var msgs []*redis.Z
	for _, z := range res {
		s, err := cast.ToStringE(z.Member)
		if err != nil {
			return fmt.Errorf("could not cast to string: %v", err)
		}
		msg, err := deserialize(s)
		if err != nil {
			return err
		}
		encoded, err := base.EncodeMessage(msg)
		if err != nil {
			return fmt.Errorf("could not encode message from %q: %v", key, err)
		}
		msgs = append(msgs, &redis.Z{Score: z.Score, Member: encoded})
	}
	if err := c.Rename(key, key+":backup").Err(); err != nil {
		return fmt.Errorf("could not rename key %q: %v", key, err)
	}
	if err := c.ZAdd(key, msgs...).Err(); err != nil {
		return fmt.Errorf("could not write new messages to %q: %v", key, err)
	}
	if err := c.Del(key + ":backup").Err(); err != nil {
		return fmt.Errorf("could not delete back up key %q: %v", key+":backup", err)
	}
	return nil
}

func migrateList(c *redis.Client, key string) error {
	if c.Exists(key).Val() == 0 {
		// skip if key doesn't exist.
		return nil
	}
	res, err := c.LRange(key, 0, -1).Result()
	if err != nil {
		return err
	}
	var msgs []interface{}
	for _, s := range res {
		msg, err := deserialize(s)
		if err != nil {
			return err
		}
		encoded, err := base.EncodeMessage(msg)
		if err != nil {
			return fmt.Errorf("could not encode message from %q: %v", key, err)
		}
		msgs = append(msgs, encoded)
	}
	if err := c.Rename(key, key+":backup").Err(); err != nil {
		return fmt.Errorf("could not rename key %q: %v", key, err)
	}
	if err := c.LPush(key, msgs...).Err(); err != nil {
		return fmt.Errorf("could not write new messages to %q: %v", key, err)
	}
	if err := c.Del(key + ":backup").Err(); err != nil {
		return fmt.Errorf("could not delete back up key %q: %v", key+":backup", err)
	}
	return nil
}

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

// migrateCmd represents the migrate command
var migrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: fmt.Sprintf("Migrate all tasks to be compatible with asynq@%s", base.Version),
	Long:  fmt.Sprintf("Migrate (asynq migrate) will convert all tasks in redis to be compatible with asynq@%s.", base.Version),
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

	lists := []string{base.InProgressQueue}
	allQueues, err := c.SMembers(base.AllQueues).Result()
	if err != nil {
		fmt.Printf("error: could not read all queues: %v", err)
		os.Exit(1)
	}
	lists = append(lists, allQueues...)
	for _, key := range lists {
		if err := migrateList(c, key); err != nil {
			fmt.Printf("error: %v", err)
			os.Exit(1)
		}
	}

	zsets := []string{base.ScheduledQueue, base.RetryQueue, base.DeadQueue}
	for _, key := range zsets {
		if err := migrateZSet(c, key); err != nil {
			fmt.Printf("error: %v", err)
			os.Exit(1)
		}
	}
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

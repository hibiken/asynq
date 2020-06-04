// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package cmd

import (
	"fmt"
	"os"

	"github.com/go-redis/redis/v7"
	"github.com/hibiken/asynq/internal/rdb"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// pauseCmd represents the pause command
var pauseCmd = &cobra.Command{
	Use:   "pause [queue name]",
	Short: "Pauses the specified queue",
	Long: `Pause (asynq pause) will pause the specified queue.
Asynq servers will not process tasks from paused queues.
Use the "unpause" command to resume a paused queue.

Example: asynq pause default -> Pause the "default" queue`,
	Args: cobra.ExactValidArgs(1),
	Run:  pause,
}

func init() {
	rootCmd.AddCommand(pauseCmd)
}

func pause(cmd *cobra.Command, args []string) {
	c := redis.NewClient(&redis.Options{
		Addr:     viper.GetString("uri"),
		DB:       viper.GetInt("db"),
		Password: viper.GetString("password"),
	})
	r := rdb.NewRDB(c)
	err := r.Pause(args[0])
	if err != nil {
		fmt.Printf("error: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Successfully paused queue %q\n", args[0])
}

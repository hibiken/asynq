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

var enqallValidArgs = []string{"scheduled", "retry", "dead"}

// enqallCmd represents the enqall command
var enqallCmd = &cobra.Command{
	Use:   "enqall [state]",
	Short: "Enqueues all tasks in the specified state",
	Long: `Enqall (asynq enqall) will enqueue all tasks in the specified state.

The argument should be one of "scheduled", "retry", or "dead".

The tasks enqueued by this command will be processed as soon as it
gets dequeued by a processor.

Example: asynq enqall dead -> Enqueues all dead tasks`,
	ValidArgs: enqallValidArgs,
	Args:      cobra.ExactValidArgs(1),
	Run:       enqall,
}

func init() {
	rootCmd.AddCommand(enqallCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// enqallCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// enqallCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

func enqall(cmd *cobra.Command, args []string) {
	c := redis.NewClient(&redis.Options{
		Addr:     viper.GetString("uri"),
		DB:       viper.GetInt("db"),
		Password: viper.GetString("password"),
	})
	r := rdb.NewRDB(c)
	var n int64
	var err error
	switch args[0] {
	case "scheduled":
		n, err = r.EnqueueAllScheduledTasks()
	case "retry":
		n, err = r.EnqueueAllRetryTasks()
	case "dead":
		n, err = r.EnqueueAllDeadTasks()
	default:
		fmt.Printf("error: `asynq enqall [state]` only accepts %v as the argument.\n", enqallValidArgs)
		os.Exit(1)
	}
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	fmt.Printf("Enqueued %d tasks in %q state\n", n, args[0])
}

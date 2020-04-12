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

var killallValidArgs = []string{"scheduled", "retry"}

// killallCmd represents the killall command
var killallCmd = &cobra.Command{
	Use:   "killall [state]",
	Short: "Kills all tasks in the specified state",
	Long: `Killall (asynq killall) will update all tasks from the specified state to dead state.

The argument should be either "scheduled" or "retry".

Example: asynq killall retry -> Update all retry tasks to dead tasks`,
	ValidArgs: killallValidArgs,
	Args:      cobra.ExactValidArgs(1),
	Run:       killall,
}

func init() {
	rootCmd.AddCommand(killallCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// killallCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// killallCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

func killall(cmd *cobra.Command, args []string) {
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
		n, err = r.KillAllScheduledTasks()
	case "retry":
		n, err = r.KillAllRetryTasks()
	default:
		fmt.Printf("error: `asynq killall [state]` only accepts %v as the argument.\n", killallValidArgs)
		os.Exit(1)
	}
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	fmt.Printf("Successfully updated %d tasks to \"dead\" state\n", n)
}

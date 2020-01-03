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
)

// killCmd represents the kill command
var killCmd = &cobra.Command{
	Use:   "kill [task id]",
	Short: "Sends a task to dead queue given an identifier",
	Long: `Kill (asynqmon kill) will send a task to dead queue given an identifier.

The command takes one argument which specifies the task to kill.
The task should be in either scheduled or retry queue.
Identifier for a task should be obtained by running "asynqmon ls" command.

Example: asynqmon kill r:1575732274:bnogo8gt6toe23vhef0g`,
	Args: cobra.ExactArgs(1),
	Run:  kill,
}

func init() {
	rootCmd.AddCommand(killCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// killCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// killCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

func kill(cmd *cobra.Command, args []string) {
	id, score, qtype, err := parseQueryID(args[0])
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	r := rdb.NewRDB(redis.NewClient(&redis.Options{
		Addr: uri,
		DB:   db,
	}))
	switch qtype {
	case "s":
		err = r.KillScheduledTask(id, score)
	case "r":
		err = r.KillRetryTask(id, score)
	default:
		fmt.Println("invalid argument")
		os.Exit(1)
	}
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	fmt.Printf("Successfully killed %v\n", args[0])

}

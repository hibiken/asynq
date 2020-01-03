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

// delCmd represents the del command
var delCmd = &cobra.Command{
	Use:   "del [task id]",
	Short: "Deletes a task given an identifier",
	Long: `Del (asynqmon del) will delete a task given an identifier.

The command takes one argument which specifies the task to delete.
The task should be in either scheduled, retry or dead queue.
Identifier for a task should be obtained by running "asynqmon ls" command.

Example: asynqmon enq d:1575732274:bnogo8gt6toe23vhef0g`,
	Args: cobra.ExactArgs(1),
	Run:  del,
}

func init() {
	rootCmd.AddCommand(delCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// delCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// delCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

func del(cmd *cobra.Command, args []string) {
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
		err = r.DeleteScheduledTask(id, score)
	case "r":
		err = r.DeleteRetryTask(id, score)
	case "d":
		err = r.DeleteDeadTask(id, score)
	default:
		fmt.Println("invalid argument")
		os.Exit(1)
	}
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	fmt.Printf("Successfully deleted %v\n", args[0])
}

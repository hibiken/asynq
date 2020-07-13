// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package cmd

import (
	"fmt"
	"os"

	"github.com/hibiken/asynq"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// enqCmd represents the enq command
var enqCmd = &cobra.Command{
	Use:   "enq [task key]",
	Short: "Enqueues a task given an identifier",
	Long: `Enq (asynq enq) will enqueue a task given an identifier.

The command takes one argument which specifies the task to enqueue.
The task should be in either scheduled, retry or dead state.
Identifier for a task should be obtained by running "asynq ls" command.

The task enqueued by this command will be processed as soon as the task 
gets dequeued by a processor.

Example: asynq enq d:1575732274:bnogo8gt6toe23vhef0g`,
	Args: cobra.ExactArgs(1),
	Run:  enq,
}

func init() {
	rootCmd.AddCommand(enqCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// enqCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// enqCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

func enq(cmd *cobra.Command, args []string) {
	i := asynq.NewInspector(asynq.RedisClientOpt{
		Addr:     viper.GetString("uri"),
		DB:       viper.GetInt("db"),
		Password: viper.GetString("password"),
	})
	err := i.EnqueueTaskByKey(args[0])
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	fmt.Printf("Successfully enqueued %v\n", args[0])
}

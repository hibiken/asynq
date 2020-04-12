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

// cancelCmd represents the cancel command
var cancelCmd = &cobra.Command{
	Use:   "cancel [task id]",
	Short: "Sends a cancelation signal to the goroutine processing the specified task",
	Long: `Cancel (asynq cancel) will send a cancelation signal to the goroutine processing 
the specified task. 

The command takes one argument which specifies the task to cancel.
The task should be in in-progress state.
Identifier for a task should be obtained by running "asynq ls" command.

Handler implementation needs to be context aware for cancelation signal to
actually cancel the processing.

Example: asynq cancel bnogo8gt6toe23vhef0g`,
	Args: cobra.ExactArgs(1),
	Run:  cancel,
}

func init() {
	rootCmd.AddCommand(cancelCmd)
}

func cancel(cmd *cobra.Command, args []string) {
	r := rdb.NewRDB(redis.NewClient(&redis.Options{
		Addr:     viper.GetString("uri"),
		DB:       viper.GetInt("db"),
		Password: viper.GetString("password"),
	}))

	err := r.PublishCancelation(args[0])
	if err != nil {
		fmt.Printf("could not send cancelation signal: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Successfully sent cancelation siganl for task %s\n", args[0])
}

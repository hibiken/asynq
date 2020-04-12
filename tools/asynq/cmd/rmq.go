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

// rmqCmd represents the rmq command
var rmqCmd = &cobra.Command{
	Use:   "rmq [queue name]",
	Short: "Removes the specified queue",
	Long: `Rmq (asynq rmq) will remove the specified queue.
By default, it will remove the queue only if it's empty.
Use --force option to override this behavior.

Example: asynq rmq low -> Removes "low" queue`,
	Args: cobra.ExactValidArgs(1),
	Run:  rmq,
}

var rmqForce bool

func init() {
	rootCmd.AddCommand(rmqCmd)
	rmqCmd.Flags().BoolVarP(&rmqForce, "force", "f", false, "remove the queue regardless of its size")
}

func rmq(cmd *cobra.Command, args []string) {
	c := redis.NewClient(&redis.Options{
		Addr:     viper.GetString("uri"),
		DB:       viper.GetInt("db"),
		Password: viper.GetString("password"),
	})
	r := rdb.NewRDB(c)
	err := r.RemoveQueue(args[0], rmqForce)
	if err != nil {
		if _, ok := err.(*rdb.ErrQueueNotEmpty); ok {
			fmt.Printf("error: %v\nIf you are sure you want to delete it, run 'asynq rmq --force %s'\n", err, args[0])
			os.Exit(1)
		}
		fmt.Printf("error: %v", err)
		os.Exit(1)
	}
	fmt.Printf("Successfully removed queue %q\n", args[0])
}

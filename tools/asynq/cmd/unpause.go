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

// unpauseCmd represents the unpause command
var unpauseCmd = &cobra.Command{
	Use:   "unpause [queue name]",
	Short: "Unpauses the specified queue",
	Long: `Unpause (asynq unpause) will unpause the specified queue.
Asynq servers will process tasks from unpaused/resumed queues.

Example: asynq unpause default -> Resume the "default" queue`,
	Args: cobra.ExactValidArgs(1),
	Run:  unpause,
}

func init() {
	rootCmd.AddCommand(unpauseCmd)
}

func unpause(cmd *cobra.Command, args []string) {
	c := redis.NewClient(&redis.Options{
		Addr:     viper.GetString("uri"),
		DB:       viper.GetInt("db"),
		Password: viper.GetString("password"),
	})
	r := rdb.NewRDB(c)
	err := r.Unpause(args[0])
	if err != nil {
		fmt.Printf("error: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Successfully resumed queue %q\n", args[0])
}

// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package cmd

import (
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/go-redis/redis/v7"
	"github.com/hibiken/asynq/internal/rdb"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// workersCmd represents the workers command
var workersCmd = &cobra.Command{
	Use:   "workers",
	Short: "Shows all running workers information",
	Long: `Workers (asynqmon workers) will show all running workers information.

The command shows the follwoing for each worker:
* Process in which the worker is running
* ID of the task worker is processing
* Type of the task worker is processing
* Payload of the task worker is processing
* Queue that the task was pulled from.
* Time the worker started processing the task`,
	Args: cobra.NoArgs,
	Run:  workers,
}

func init() {
	rootCmd.AddCommand(workersCmd)
}

func workers(cmd *cobra.Command, args []string) {
	r := rdb.NewRDB(redis.NewClient(&redis.Options{
		Addr:     viper.GetString("uri"),
		DB:       viper.GetInt("db"),
		Password: viper.GetString("password"),
	}))

	workers, err := r.ListWorkers()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	if len(workers) == 0 {
		fmt.Println("No workers")
		return
	}

	// sort by started timestamp or ID.
	sort.Slice(workers, func(i, j int) bool {
		x, y := workers[i], workers[j]
		if x.Started != y.Started {
			return x.Started.Before(y.Started)
		}
		return x.ID.String() < y.ID.String()
	})

	cols := []string{"Process", "ID", "Type", "Payload", "Queue", "Started"}
	printRows := func(w io.Writer, tmpl string) {
		for _, wk := range workers {
			fmt.Fprintf(w, tmpl,
				fmt.Sprintf("%s:%d", wk.Host, wk.PID), wk.ID, wk.Type, wk.Payload, wk.Queue, timeAgo(wk.Started))
		}
	}
	printTable(cols, printRows)
}

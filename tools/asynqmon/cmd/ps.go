// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package cmd

import (
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/hibiken/asynq/internal/rdb"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// psCmd represents the ps command
var psCmd = &cobra.Command{
	Use:   "ps",
	Short: "Shows all background worker processes",
	Long: `Ps (asynqmon ps) will show all background worker processes
backed by the specified redis instance.

The command shows the following for each process:
* Host and PID of the process
* Number of active workers out of worker pool
* Queues configuration
* State of the process ("running" | "stopped")

A "running" process is processing tasks in queues.
A "stopped" process are no longer processing new tasks.`,
	Args: cobra.NoArgs,
	Run:  ps,
}

func init() {
	rootCmd.AddCommand(psCmd)
}

func ps(cmd *cobra.Command, args []string) {
	c := redis.NewClient(&redis.Options{
		Addr:     viper.GetString("uri"),
		DB:       viper.GetInt("db"),
		Password: viper.GetString("password"),
	})
	r := rdb.NewRDB(c)

	processes, err := r.ListProcesses()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	if len(processes) == 0 {
		fmt.Println("No processes")
		return
	}
	cols := []string{"Host", "PID", "State", "Active Workers", "Queues", "Started"}
	printRows := func(w io.Writer, tmpl string) {
		for _, ps := range processes {
			fmt.Fprintf(w, tmpl,
				ps.Host, ps.PID, ps.State,
				fmt.Sprintf("%d/%d", ps.ActiveWorkerCount, ps.Concurrency),
				formatQueues(ps.Queues), timeAgo(ps.Started))
		}
	}
	printTable(cols, printRows)
}

// timeAgo takes a time and returns a string of the format "<duration> ago".
func timeAgo(since time.Time) string {
	d := time.Since(since).Round(time.Second)
	return fmt.Sprintf("%v ago", d)
}

func formatQueues(queues map[string]uint) string {
	var b strings.Builder
	l := len(queues)
	for qname, p := range queues {
		fmt.Fprintf(&b, "%s:%d", qname, p)
		l--
		if l > 0 {
			b.WriteString(" ")
		}
	}
	return b.String()
}

// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package cmd

import (
	"fmt"
	"io"
	"os"
	"sort"
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
* Queue configuration
* State of the worker process ("running" | "stopped")
* Time the process was started

A "running" process is processing tasks in queues.
A "stopped" process is no longer processing new tasks.`,
	Args: cobra.NoArgs,
	Run:  ps,
}

func init() {
	rootCmd.AddCommand(psCmd)
}

func ps(cmd *cobra.Command, args []string) {
	r := rdb.NewRDB(redis.NewClient(&redis.Options{
		Addr:     viper.GetString("uri"),
		DB:       viper.GetInt("db"),
		Password: viper.GetString("password"),
	}))

	processes, err := r.ListProcesses()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	if len(processes) == 0 {
		fmt.Println("No processes")
		return
	}

	// sort by hostname and pid
	sort.Slice(processes, func(i, j int) bool {
		x, y := processes[i], processes[j]
		if x.Host != y.Host {
			return x.Host < y.Host
		}
		return x.PID < y.PID
	})

	// print processes
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

func formatQueues(qmap map[string]int) string {
	// sort queues by priority and name
	type queue struct {
		name     string
		priority int
	}
	var queues []*queue
	for qname, p := range qmap {
		queues = append(queues, &queue{qname, p})
	}
	sort.Slice(queues, func(i, j int) bool {
		x, y := queues[i], queues[j]
		if x.priority != y.priority {
			return x.priority > y.priority
		}
		return x.name < y.name
	})

	var b strings.Builder
	l := len(queues)
	for _, q := range queues {
		fmt.Fprintf(&b, "%s:%d", q.name, q.priority)
		l--
		if l > 0 {
			b.WriteString(" ")
		}
	}
	return b.String()
}

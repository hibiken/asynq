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

	"github.com/MakeNowJust/heredoc/v2"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(serverCmd)
	serverCmd.AddCommand(serverListCmd)
}

var serverCmd = &cobra.Command{
	Use:   "server <command> [flags]",
	Short: "Manage servers",
	Example: heredoc.Doc(`
		$ asynq server list`),
}

var serverListCmd = &cobra.Command{
	Use:     "list",
	Aliases: []string{"ls"},
	Short:   "List servers",
	Long: `Server list (asynq server ls) shows all running worker servers
pulling tasks from the given redis instance.

The command shows the following for each server:
* Host and PID of the process in which the server is running
* Number of active workers out of worker pool
* Queue configuration
* State of the worker server ("active" | "stopped")
* Time the server was started

A "active" server is pulling tasks from queues and processing them.
A "stopped" server is no longer pulling new tasks from queues`,
	Run: serverList,
}

func serverList(cmd *cobra.Command, args []string) {
	r := createRDB()

	servers, err := r.ListServers()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	if len(servers) == 0 {
		fmt.Println("No running servers")
		return
	}

	// sort by hostname and pid
	sort.Slice(servers, func(i, j int) bool {
		x, y := servers[i], servers[j]
		if x.Host != y.Host {
			return x.Host < y.Host
		}
		return x.PID < y.PID
	})

	// print server info
	cols := []string{"Host", "PID", "State", "Active Workers", "Queues", "Started"}
	printRows := func(w io.Writer, tmpl string) {
		for _, info := range servers {
			fmt.Fprintf(w, tmpl,
				info.Host, info.PID, info.Status,
				fmt.Sprintf("%d/%d", info.ActiveWorkerCount, info.Concurrency),
				formatQueues(info.Queues), timeAgo(info.Started))
		}
	}
	printTable(cols, printRows)
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

// timeAgo takes a time and returns a string of the format "<duration> ago".
func timeAgo(since time.Time) string {
	d := time.Since(since).Round(time.Second)
	return fmt.Sprintf("%v ago", d)
}

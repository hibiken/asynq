// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package cmd

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/hibiken/asynq/internal/rdb"
	"github.com/spf13/cobra"
)

// statsCmd represents the stats command
var statsCmd = &cobra.Command{
	Use:   "stats",
	Short: "Shows current state of the tasks and queues",
	Long: `Stats (aysnqmon stats) will show the overview of tasks and queues at that instant.

Specifically, the command shows the following:
* Number of tasks in each state
* Number of tasks in each queue
* Aggregate data for the current day
* Basic information about the running redis instance

To monitor the tasks continuously, it's recommended that you run this
command in conjunction with the watch command.

Example: watch -n 3 asynq stats -> Shows current state of tasks every three seconds`,
	Args: cobra.NoArgs,
	Run:  stats,
}

func init() {
	rootCmd.AddCommand(statsCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// statsCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// statsCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

type AggregateStats struct {
	Active    int
	Pending   int
	Scheduled int
	Retry     int
	Dead      int
	Processed int
	Failed    int
	Timestamp time.Time
}

func stats(cmd *cobra.Command, args []string) {
	r := createRDB()

	queues, err := r.AllQueues()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	var aggStats AggregateStats
	var stats []*rdb.Stats
	for _, qname := range queues {
		s, err := r.CurrentStats(qname)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		aggStats.Active += s.Active
		aggStats.Pending += s.Pending
		aggStats.Scheduled += s.Scheduled
		aggStats.Retry += s.Retry
		aggStats.Dead += s.Dead
		aggStats.Processed += s.Processed
		aggStats.Failed += s.Failed
		aggStats.Timestamp = s.Timestamp
		stats = append(stats, s)
	}
	var info map[string]string
	if useRedisCluster {
		info, err = r.RedisClusterInfo()
	} else {
		info, err = r.RedisInfo()
	}
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	fmt.Println("BY STATES")
	printStatsByState(&aggStats)
	fmt.Println()

	fmt.Println("BY QUEUES")
	printStatsByQueue(stats)
	fmt.Println()

	fmt.Printf("STATS FOR %s UTC\n", aggStats.Timestamp.UTC().Format("2006-01-02"))
	printSuccessFailureStats(&aggStats)
	fmt.Println()

	if useRedisCluster {
		fmt.Println("REDIS CLUSTER INFO")
		printClusterInfo(info)
	} else {
		fmt.Println("REDIS INFO")
		printInfo(info)
	}
	fmt.Println()
}

func printStatsByState(s *AggregateStats) {
	format := strings.Repeat("%v\t", 5) + "\n"
	tw := new(tabwriter.Writer).Init(os.Stdout, 0, 8, 2, ' ', 0)
	fmt.Fprintf(tw, format, "Active", "Pending", "Scheduled", "Retry", "Dead")
	fmt.Fprintf(tw, format, "----------", "--------", "---------", "-----", "----")
	fmt.Fprintf(tw, format, s.Active, s.Pending, s.Scheduled, s.Retry, s.Dead)
	tw.Flush()
}

func printStatsByQueue(stats []*rdb.Stats) {
	var headers, seps, counts []string
	for _, s := range stats {
		title := queueTitle(s)
		headers = append(headers, title)
		seps = append(seps, strings.Repeat("-", len(title)))
		counts = append(counts, strconv.Itoa(s.Size))
	}
	format := strings.Repeat("%v\t", len(headers)) + "\n"
	tw := new(tabwriter.Writer).Init(os.Stdout, 0, 8, 2, ' ', 0)
	fmt.Fprintf(tw, format, toInterfaceSlice(headers)...)
	fmt.Fprintf(tw, format, toInterfaceSlice(seps)...)
	fmt.Fprintf(tw, format, toInterfaceSlice(counts)...)
	tw.Flush()
}

func queueTitle(s *rdb.Stats) string {
	var b strings.Builder
	b.WriteString(strings.Title(s.Queue))
	if s.Paused {
		b.WriteString(" (Paused)")
	}
	return b.String()
}

func printSuccessFailureStats(s *AggregateStats) {
	format := strings.Repeat("%v\t", 3) + "\n"
	tw := new(tabwriter.Writer).Init(os.Stdout, 0, 8, 2, ' ', 0)
	fmt.Fprintf(tw, format, "Processed", "Failed", "Error Rate")
	fmt.Fprintf(tw, format, "---------", "------", "----------")
	var errrate string
	if s.Processed == 0 {
		errrate = "N/A"
	} else {
		errrate = fmt.Sprintf("%.2f%%", float64(s.Failed)/float64(s.Processed)*100)
	}
	fmt.Fprintf(tw, format, s.Processed, s.Failed, errrate)
	tw.Flush()
}

func printInfo(info map[string]string) {
	format := strings.Repeat("%v\t", 5) + "\n"
	tw := new(tabwriter.Writer).Init(os.Stdout, 0, 8, 2, ' ', 0)
	fmt.Fprintf(tw, format, "Version", "Uptime", "Connections", "Memory Usage", "Peak Memory Usage")
	fmt.Fprintf(tw, format, "-------", "------", "-----------", "------------", "-----------------")
	fmt.Fprintf(tw, format,
		info["redis_version"],
		fmt.Sprintf("%s days", info["uptime_in_days"]),
		info["connected_clients"],
		fmt.Sprintf("%sB", info["used_memory_human"]),
		fmt.Sprintf("%sB", info["used_memory_peak_human"]),
	)
	tw.Flush()
}

func printClusterInfo(info map[string]string) {
	printTable(
		[]string{"State", "Known Nodes", "Cluster Size"},
		func(w io.Writer, tmpl string) {
			fmt.Fprintf(w, tmpl,
				strings.ToUpper(info["cluster_state"]),
				info["cluster_known_nodes"],
				info["cluster_size"],
			)
		},
	)
}

func toInterfaceSlice(strs []string) []interface{} {
	var res []interface{}
	for _, s := range strs {
		res = append(res, s)
	}
	return res
}

// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"
	"unicode/utf8"

	"github.com/Kua-Fu/asynq/internal/rdb"
	"github.com/MakeNowJust/heredoc/v2"
	"github.com/fatih/color"
	"github.com/spf13/cobra"
)

// statsCmd represents the stats command
var statsCmd = &cobra.Command{
	Use:   "stats",
	Short: "View current state",
	Long: heredoc.Doc(`
	  Stats shows the overview of tasks and queues at that instant.

	  The command shows the following:
	    * Number of tasks in each state
	    * Number of tasks in each queue
	    * Aggregate data for the current day
	    * Basic information about the running redis instance`),
	Args: cobra.NoArgs,
	Run:  stats,
}

var jsonFlag bool

func init() {
	rootCmd.AddCommand(statsCmd)
	statsCmd.Flags().BoolVar(&jsonFlag, "json", false, "Output stats in JSON format.")

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// statsCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// statsCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

type AggregateStats struct {
	Active      int       `json:"active"`
	Pending     int       `json:"pending"`
	Aggregating int       `json:"aggregating"`
	Scheduled   int       `json:"scheduled"`
	Retry       int       `json:"retry"`
	Archived    int       `json:"archived"`
	Completed   int       `json:"completed"`
	Processed   int       `json:"processed"`
	Failed      int       `json:"failed"`
	Timestamp   time.Time `json:"timestamp"`
}

type FullStats struct {
	Aggregate  AggregateStats    `json:"aggregate"`
	QueueStats []*rdb.Stats      `json:"queues"`
	RedisInfo  map[string]string `json:"redis"`
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
		aggStats.Aggregating += s.Aggregating
		aggStats.Scheduled += s.Scheduled
		aggStats.Retry += s.Retry
		aggStats.Archived += s.Archived
		aggStats.Completed += s.Completed
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

	if jsonFlag {
		statsJSON, err := json.Marshal(FullStats{
			Aggregate:  aggStats,
			QueueStats: stats,
			RedisInfo:  info,
		})

		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		fmt.Println(string(statsJSON))
		return
	}

	bold := color.New(color.Bold)
	bold.Println("Task Count by State")
	printStatsByState(&aggStats)
	fmt.Println()

	bold.Println("Task Count by Queue")
	printStatsByQueue(stats)
	fmt.Println()

	bold.Printf("Daily Stats %s UTC\n", aggStats.Timestamp.UTC().Format("2006-01-02"))
	printSuccessFailureStats(&aggStats)
	fmt.Println()

	if useRedisCluster {
		bold.Println("Redis Cluster Info")
		printClusterInfo(info)
	} else {
		bold.Println("Redis Info")
		printInfo(info)
	}
	fmt.Println()
}

func printStatsByState(s *AggregateStats) {
	format := strings.Repeat("%v\t", 7) + "\n"
	tw := new(tabwriter.Writer).Init(os.Stdout, 0, 8, 2, ' ', 0)
	fmt.Fprintf(tw, format, "active", "pending", "aggregating", "scheduled", "retry", "archived", "completed")
	width := maxInt(9 /* defaultWidth */, maxWidthOf(s.Active, s.Pending, s.Aggregating, s.Scheduled, s.Retry, s.Archived, s.Completed)) // length of widest column
	sep := strings.Repeat("-", width)
	fmt.Fprintf(tw, format, sep, sep, sep, sep, sep, sep, sep)
	fmt.Fprintf(tw, format, s.Active, s.Pending, s.Aggregating, s.Scheduled, s.Retry, s.Archived, s.Completed)
	tw.Flush()
}

// numDigits returns the number of digits in n.
func numDigits(n int) int {
	return len(strconv.Itoa(n))
}

// maxWidthOf returns the max number of digits amount the provided vals.
func maxWidthOf(vals ...int) int {
	max := 0
	for _, v := range vals {
		if vw := numDigits(v); vw > max {
			max = vw
		}
	}
	return max
}

func maxInt(a, b int) int {
	return int(math.Max(float64(a), float64(b)))
}

func printStatsByQueue(stats []*rdb.Stats) {
	var headers, seps, counts []string
	maxHeaderWidth := 0
	for _, s := range stats {
		title := queueTitle(s)
		headers = append(headers, title)
		if w := utf8.RuneCountInString(title); w > maxHeaderWidth {
			maxHeaderWidth = w
		}
		counts = append(counts, strconv.Itoa(s.Size))
	}
	for i := 0; i < len(headers); i++ {
		seps = append(seps, strings.Repeat("-", maxHeaderWidth))
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
	b.WriteString(s.Queue)
	if s.Paused {
		b.WriteString(" (paused)")
	}
	return b.String()
}

func printSuccessFailureStats(s *AggregateStats) {
	format := strings.Repeat("%v\t", 3) + "\n"
	tw := new(tabwriter.Writer).Init(os.Stdout, 0, 8, 2, ' ', 0)
	fmt.Fprintf(tw, format, "processed", "failed", "error rate")
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
	fmt.Fprintf(tw, format, "version", "uptime", "connections", "memory usage", "peak memory usage")
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

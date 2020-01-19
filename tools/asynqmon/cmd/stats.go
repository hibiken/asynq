// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package cmd

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"

	"github.com/go-redis/redis/v7"
	"github.com/hibiken/asynq/internal/rdb"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// statsCmd represents the stats command
var statsCmd = &cobra.Command{
	Use:   "stats",
	Short: "Shows current state of the queues",
	Long: `Stats (aysnqmon stats) will show the number of tasks in each queue at that instant.
It also displays basic information about the running redis instance.

To monitor the queues continuously, it's recommended that you run this
command in conjunction with the watch command.

Example: watch -n 5 asynqmon stats`,
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

func stats(cmd *cobra.Command, args []string) {
	c := redis.NewClient(&redis.Options{
		Addr:     viper.GetString("uri"),
		DB:       viper.GetInt("db"),
		Password: viper.GetString("password"),
	})
	r := rdb.NewRDB(c)

	stats, err := r.CurrentStats()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	info, err := r.RedisInfo()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	fmt.Println("STATES")
	printStates(stats)
	fmt.Println()

	fmt.Println("QUEUES")
	printQueues(stats.Queues)
	fmt.Println()

	fmt.Printf("STATS FOR %s UTC\n", stats.Timestamp.UTC().Format("2006-01-02"))
	printStats(stats)
	fmt.Println()

	fmt.Println("REDIS INFO")
	printInfo(info)
	fmt.Println()
}

func printStates(s *rdb.Stats) {
	format := strings.Repeat("%v\t", 5) + "\n"
	tw := new(tabwriter.Writer).Init(os.Stdout, 0, 8, 2, ' ', 0)
	fmt.Fprintf(tw, format, "InProgress", "Enqueued", "Scheduled", "Retry", "Dead")
	fmt.Fprintf(tw, format, "----------", "--------", "---------", "-----", "----")
	fmt.Fprintf(tw, format, s.InProgress, s.Enqueued, s.Scheduled, s.Retry, s.Dead)
	tw.Flush()
}

func printQueues(queues map[string]int) {
	var qnames, seps, counts []string
	for q := range queues {
		qnames = append(qnames, strings.Title(q))
	}
	sort.Strings(qnames) // sort for stable order
	for _, q := range qnames {
		seps = append(seps, strings.Repeat("-", len(q)))
		counts = append(counts, strconv.Itoa(queues[strings.ToLower(q)]))
	}
	format := strings.Repeat("%v\t", len(qnames)) + "\n"
	tw := new(tabwriter.Writer).Init(os.Stdout, 0, 8, 2, ' ', 0)
	fmt.Fprintf(tw, format, toInterfaceSlice(qnames)...)
	fmt.Fprintf(tw, format, toInterfaceSlice(seps)...)
	fmt.Fprintf(tw, format, toInterfaceSlice(counts)...)
	tw.Flush()
}

func printStats(s *rdb.Stats) {
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

func toInterfaceSlice(strs []string) []interface{} {
	var res []interface{}
	for _, s := range strs {
		res = append(res, s)
	}
	return res
}

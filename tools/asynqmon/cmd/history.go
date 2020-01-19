// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package cmd

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"

	"github.com/go-redis/redis/v7"
	"github.com/hibiken/asynq/internal/rdb"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// historyCmd represents the history command
var historyCmd = &cobra.Command{
	Use:   "history [num of days]",
	Short: "Shows historical aggregate data",
	Long: `History (asynqmon history) will show the number of processed tasks
as well as the error rate for the last n days.

Example: asynqmon history 7 -> Shows stats from the last 7 days`,
	Args: cobra.ExactArgs(1),
	Run:  history,
}

func init() {
	rootCmd.AddCommand(historyCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// historyCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// historyCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

func history(cmd *cobra.Command, args []string) {
	n, err := strconv.Atoi(args[0])
	if err != nil {
		fmt.Printf(`Error: Invalid argument. Argument has to be an integer.

Usage: asynqmon history [num of days]
`)
		os.Exit(1)
	}
	if err != nil {

	}
	c := redis.NewClient(&redis.Options{
		Addr:     viper.GetString("uri"),
		DB:       viper.GetInt("db"),
		Password: viper.GetString("password"),
	})
	r := rdb.NewRDB(c)

	stats, err := r.HistoricalStats(n)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	printDailyStats(stats)
}

func printDailyStats(stats []*rdb.DailyStats) {
	format := strings.Repeat("%v\t", 4) + "\n"
	tw := new(tabwriter.Writer).Init(os.Stdout, 0, 8, 2, ' ', 0)
	fmt.Fprintf(tw, format, "Date (UTC)", "Processed", "Failed", "Error Rate")
	fmt.Fprintf(tw, format, "----------", "---------", "------", "----------")
	for _, s := range stats {
		var errrate string
		if s.Processed == 0 {
			errrate = "N/A"
		} else {
			errrate = fmt.Sprintf("%.2f%%", float64(s.Failed)/float64(s.Processed)*100)
		}
		fmt.Fprintf(tw, format, s.Time.Format("2006-01-02"), s.Processed, s.Failed, errrate)
	}
	tw.Flush()
}

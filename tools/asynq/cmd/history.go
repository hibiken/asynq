// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package cmd

import (
	"fmt"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/go-redis/redis/v7"
	"github.com/hibiken/asynq/internal/rdb"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var days int

// historyCmd represents the history command
var historyCmd = &cobra.Command{
	Use:   "history",
	Short: "Shows historical aggregate data",
	Long: `History (asynq history) will show the number of processed and failed tasks
from the last x days.

By default, it will show the data from the last 10 days.

Example: asynq history -x=30 -> Shows stats from the last 30 days`,
	Args: cobra.NoArgs,
	Run:  history,
}

func init() {
	rootCmd.AddCommand(historyCmd)
	historyCmd.Flags().IntVarP(&days, "days", "x", 10, "show data from last x days")
}

func history(cmd *cobra.Command, args []string) {
	c := redis.NewClient(&redis.Options{
		Addr:     viper.GetString("uri"),
		DB:       viper.GetInt("db"),
		Password: viper.GetString("password"),
	})
	r := rdb.NewRDB(c)

	stats, err := r.HistoricalStats(days)
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

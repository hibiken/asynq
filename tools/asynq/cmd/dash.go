// Copyright 2022 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package cmd

import (
	"time"

	"github.com/MakeNowJust/heredoc/v2"
	"github.com/hibiken/asynq/tools/asynq/cmd/dash"
	"github.com/spf13/cobra"
)

var (
	flagDebug        = false
	flagUseRealData  = false
	flagPollInterval = 8 * time.Second
)

func init() {
	rootCmd.AddCommand(dashCmd)
	dashCmd.Flags().DurationVar(&flagPollInterval, "refresh", 8*time.Second, "Interval between data refresh")
	// TODO: Remove this debug once we're done
	dashCmd.Flags().BoolVar(&flagDebug, "debug", false, "Print debug info")
	dashCmd.Flags().BoolVar(&flagUseRealData, "realdata", true, "Use real data in redis")
}

var dashCmd = &cobra.Command{
	Use:   "dash",
	Short: "View dashboard",
	Long: heredoc.Doc(`
		Displays dashboard.`),
	Args: cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		dash.Run(dash.Options{
			DebugMode:    flagDebug,
			UseRealData:  flagUseRealData,
			PollInterval: flagPollInterval,
		})
	},
}

// Copyright 2022 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package cmd

import (
	"fmt"
	"time"

	"github.com/MakeNowJust/heredoc/v2"
	"github.com/hibiken/asynq/tools/asynq/cmd/dash"
	"github.com/spf13/cobra"
)

var (
	flagPollInterval = 8 * time.Second
)

func init() {
	rootCmd.AddCommand(dashCmd)
	dashCmd.Flags().DurationVar(&flagPollInterval, "refresh", 8*time.Second, "Interval between data refresh (default: 8s, min allowed: 1s)")
}

var dashCmd = &cobra.Command{
	Use:   "dash",
	Short: "View dashboard",
	Long: heredoc.Doc(`
		Display interactive dashboard.`),
	Args: cobra.NoArgs,
	Example: heredoc.Doc(`
        $ asynq dash
        $ asynq dash --refresh=3s`),
	RunE: func(cmd *cobra.Command, args []string) error {
		if flagPollInterval < 1*time.Second {
			return fmt.Errorf("--refresh cannot be less than 1s")
		}
		dash.Run(dash.Options{
			PollInterval: flagPollInterval,
			RedisConnOpt: getRedisConnOpt(),
		})
		return nil
	},
}

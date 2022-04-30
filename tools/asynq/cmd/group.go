// Copyright 2022 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package cmd

import (
	"fmt"
	"os"

	"github.com/MakeNowJust/heredoc/v2"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(groupCmd)
	groupCmd.AddCommand(groupListCmd)
	groupListCmd.Flags().StringP("queue", "q", "", "queue to inspect")
	groupListCmd.MarkFlagRequired("queue")
}

var groupCmd = &cobra.Command{
	Use:   "group <command> [flags]",
	Short: "Manage groups",
	Example: heredoc.Doc(`
		$ asynq group list --queue=myqueue`),
}

var groupListCmd = &cobra.Command{
	Use:     "list",
	Aliases: []string{"ls"},
	Short:   "List groups",
	Args:    cobra.NoArgs,
	Run:     groupLists,
}

func groupLists(cmd *cobra.Command, args []string) {
	qname, err := cmd.Flags().GetString("queue")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	inspector := createInspector()
	groups, err := inspector.Groups(qname)
	if len(groups) == 0 {
		fmt.Printf("No groups found in queue %q\n", qname)
		return
	}
	for _, g := range groups {
		fmt.Println(g.Group)
	}
}

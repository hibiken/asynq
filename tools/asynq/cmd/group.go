// Copyright 2022 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package cmd

import (
	"fmt"

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
	RunE:    groupLists,
}

func groupLists(cmd *cobra.Command, args []string) error {
	qname, err := cmd.Flags().GetString("queue")
	if err != nil {
		return err
	}
	inspector := createInspector()
	groups, err := inspector.Groups(qname)
	if err != nil {
		return fmt.Errorf("could not fetch groups: %v", err)
	}
	if len(groups) == 0 {
		fmt.Printf("No groups found in queue %q\n", qname)
		return nil
	}
	for _, g := range groups {
		fmt.Println(g.Group)
	}
	return nil
}

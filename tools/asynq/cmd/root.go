// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package cmd

import (
	"crypto/tls"
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"
	"unicode"
	"unicode/utf8"

	"github.com/Kua-Fu/asynq"
	"github.com/Kua-Fu/asynq/internal/base"
	"github.com/Kua-Fu/asynq/internal/rdb"
	"github.com/MakeNowJust/heredoc/v2"
	"github.com/fatih/color"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"golang.org/x/exp/utf8string"

	homedir "github.com/mitchellh/go-homedir"
	"github.com/spf13/viper"
)

var cfgFile string

// Global flag variables
var (
	uri      string
	db       int
	password string

	useRedisCluster bool
	clusterAddrs    string
	tlsServerName   string
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:     "asynq <command> <subcommand> [flags]",
	Short:   "Asynq CLI",
	Long:    `Command line tool to inspect tasks and queues managed by Asynq`,
	Version: base.Version,

	SilenceUsage:  true,
	SilenceErrors: true,

	Example: heredoc.Doc(`
		$ asynq stats
		$ asynq queue pause myqueue
		$ asynq task list --queue=myqueue --state=archived`),
	Annotations: map[string]string{
		"help:feedback": heredoc.Doc(`
			Open an issue at https://github.com/Kua-Fu/asynq/issues/new/choose`),
	},
}

var versionOutput = fmt.Sprintf("asynq version %s\n", base.Version)

var versionCmd = &cobra.Command{
	Use:    "version",
	Hidden: true,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Print(versionOutput)
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func isRootCmd(cmd *cobra.Command) bool {
	return cmd != nil && !cmd.HasParent()
}

// displayLine represents a line displayed in the output as '<name> <desc>',
// where pad is used to pad the name from desc.
type displayLine struct {
	name string
	desc string
	pad  int // number of rpad
}

func (l *displayLine) String() string {
	return rpad(l.name, l.pad) + l.desc
}

type displayLines []*displayLine

func (dls displayLines) String() string {
	var lines []string
	for _, dl := range dls {
		lines = append(lines, dl.String())
	}
	return strings.Join(lines, "\n")
}

// Capitalize the first word in the given string.
func capitalize(s string) string {
	str := utf8string.NewString(s)
	if str.RuneCount() == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString(strings.ToUpper(string(str.At(0))))
	b.WriteString(str.Slice(1, str.RuneCount()))
	return b.String()
}

func rootHelpFunc(cmd *cobra.Command, args []string) {
	// Display helpful error message when user mistypes a subcommand (e.g. 'asynq queue lst').
	if isRootCmd(cmd.Parent()) && len(args) >= 2 && args[1] != "--help" && args[1] != "-h" {
		printSubcommandSuggestions(cmd, args[1])
		return
	}

	var lines []*displayLine
	var commands []*displayLine
	for _, c := range cmd.Commands() {
		if c.Hidden || c.Short == "" || c.Name() == "help" {
			continue
		}
		l := &displayLine{name: c.Name() + ":", desc: capitalize(c.Short)}
		commands = append(commands, l)
		lines = append(lines, l)
	}
	var localFlags []*displayLine
	cmd.LocalFlags().VisitAll(func(f *pflag.Flag) {
		l := &displayLine{name: "--" + f.Name, desc: capitalize(f.Usage)}
		localFlags = append(localFlags, l)
		lines = append(lines, l)
	})
	var inheritedFlags []*displayLine
	cmd.InheritedFlags().VisitAll(func(f *pflag.Flag) {
		l := &displayLine{name: "--" + f.Name, desc: capitalize(f.Usage)}
		inheritedFlags = append(inheritedFlags, l)
		lines = append(lines, l)
	})
	adjustPadding(lines...)

	type helpEntry struct {
		Title string
		Body  string
	}
	var helpEntries []*helpEntry
	desc := cmd.Long
	if desc == "" {
		desc = cmd.Short
	}
	if desc != "" {
		helpEntries = append(helpEntries, &helpEntry{"", desc})
	}
	helpEntries = append(helpEntries, &helpEntry{"USAGE", cmd.UseLine()})
	if len(commands) > 0 {
		helpEntries = append(helpEntries, &helpEntry{"COMMANDS", displayLines(commands).String()})
	}
	if cmd.LocalFlags().HasFlags() {
		helpEntries = append(helpEntries, &helpEntry{"FLAGS", displayLines(localFlags).String()})
	}
	if cmd.InheritedFlags().HasFlags() {
		helpEntries = append(helpEntries, &helpEntry{"INHERITED FLAGS", displayLines(inheritedFlags).String()})
	}
	if cmd.Example != "" {
		helpEntries = append(helpEntries, &helpEntry{"EXAMPLES", cmd.Example})
	}
	helpEntries = append(helpEntries, &helpEntry{"LEARN MORE", heredoc.Doc(`
		Use 'asynq <command> <subcommand> --help' for more information about a command.`)})
	if s, ok := cmd.Annotations["help:feedback"]; ok {
		helpEntries = append(helpEntries, &helpEntry{"FEEDBACK", s})
	}

	out := cmd.OutOrStdout()
	bold := color.New(color.Bold)
	for _, e := range helpEntries {
		if e.Title != "" {
			// If there is a title, add indentation to each line in the body
			bold.Fprintln(out, e.Title)
			fmt.Fprintln(out, indent(e.Body, 2 /* spaces */))
		} else {
			// If there is no title, print the body as is
			fmt.Fprintln(out, e.Body)
		}
		fmt.Fprintln(out)
	}
}

func rootUsageFunc(cmd *cobra.Command) error {
	out := cmd.OutOrStdout()
	fmt.Fprintf(out, "Usage: %s", cmd.UseLine())
	if subcmds := cmd.Commands(); len(subcmds) > 0 {
		fmt.Fprint(out, "\n\nAvailable commands:\n")
		for _, c := range subcmds {
			if c.Hidden {
				continue
			}
			fmt.Fprintf(out, "  %s\n", c.Name())
		}
	}

	var localFlags []*displayLine
	cmd.LocalFlags().VisitAll(func(f *pflag.Flag) {
		localFlags = append(localFlags, &displayLine{name: "--" + f.Name, desc: capitalize(f.Usage)})
	})
	adjustPadding(localFlags...)
	if len(localFlags) > 0 {
		fmt.Fprint(out, "\n\nFlags:\n")
		for _, l := range localFlags {
			fmt.Fprintf(out, "  %s\n", l.String())
		}
	}
	return nil
}

func printSubcommandSuggestions(cmd *cobra.Command, arg string) {
	out := cmd.OutOrStdout()
	fmt.Fprintf(out, "unknown command %q for %q\n", arg, cmd.CommandPath())
	if cmd.SuggestionsMinimumDistance <= 0 {
		cmd.SuggestionsMinimumDistance = 2
	}
	candidates := cmd.SuggestionsFor(arg)
	if len(candidates) > 0 {
		fmt.Fprint(out, "\nDid you mean this?\n")
		for _, c := range candidates {
			fmt.Fprintf(out, "\t%s\n", c)
		}
	}
	fmt.Fprintln(out)
	rootUsageFunc(cmd)
}

func adjustPadding(lines ...*displayLine) {
	// find the maximum width of the name
	max := 0
	for _, l := range lines {
		if n := utf8.RuneCountInString(l.name); n > max {
			max = n
		}
	}
	for _, l := range lines {
		l.pad = max
	}
}

// rpad adds padding to the right of a string.
func rpad(s string, padding int) string {
	tmpl := fmt.Sprintf("%%-%ds ", padding)
	return fmt.Sprintf(tmpl, s)

}

// lpad adds padding to the left of a string.
func lpad(s string, padding int) string {
	tmpl := fmt.Sprintf("%%%ds ", padding)
	return fmt.Sprintf(tmpl, s)
}

// indent indents the given text by given spaces.
func indent(text string, space int) string {
	if len(text) == 0 {
		return ""
	}
	var b strings.Builder
	indentation := strings.Repeat(" ", space)
	lastRune := '\n'
	for _, r := range text {
		if lastRune == '\n' {
			b.WriteString(indentation)
		}
		b.WriteRune(r)
		lastRune = r
	}
	return b.String()
}

// dedent removes any indentation from the given text.
func dedent(text string) string {
	lines := strings.Split(text, "\n")
	var b strings.Builder
	for _, l := range lines {
		b.WriteString(strings.TrimLeftFunc(l, unicode.IsSpace))
		b.WriteRune('\n')
	}
	return b.String()
}

func init() {
	cobra.OnInitialize(initConfig)

	rootCmd.SetHelpFunc(rootHelpFunc)
	rootCmd.SetUsageFunc(rootUsageFunc)

	rootCmd.AddCommand(versionCmd)
	rootCmd.SetVersionTemplate(versionOutput)

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "Config file to set flag defaut values (default is $HOME/.asynq.yaml)")
	rootCmd.PersistentFlags().StringVarP(&uri, "uri", "u", "127.0.0.1:6379", "Redis server URI")
	rootCmd.PersistentFlags().IntVarP(&db, "db", "n", 0, "Redis database number (default is 0)")
	rootCmd.PersistentFlags().StringVarP(&password, "password", "p", "", "Password to use when connecting to redis server")
	rootCmd.PersistentFlags().BoolVar(&useRedisCluster, "cluster", false, "Connect to redis cluster")
	rootCmd.PersistentFlags().StringVar(&clusterAddrs, "cluster_addrs",
		"127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003,127.0.0.1:7004,127.0.0.1:7005",
		"List of comma-separated redis server addresses")
	rootCmd.PersistentFlags().StringVar(&tlsServerName, "tls_server",
		"", "Server name for TLS validation")
	// Bind flags with config.
	viper.BindPFlag("uri", rootCmd.PersistentFlags().Lookup("uri"))
	viper.BindPFlag("db", rootCmd.PersistentFlags().Lookup("db"))
	viper.BindPFlag("password", rootCmd.PersistentFlags().Lookup("password"))
	viper.BindPFlag("cluster", rootCmd.PersistentFlags().Lookup("cluster"))
	viper.BindPFlag("cluster_addrs", rootCmd.PersistentFlags().Lookup("cluster_addrs"))
	viper.BindPFlag("tls_server", rootCmd.PersistentFlags().Lookup("tls_server"))
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := homedir.Dir()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		// Search config in home directory with name ".asynq" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigName(".asynq")
	}

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}
}

// createRDB creates a RDB instance using flag values and returns it.
func createRDB() *rdb.RDB {
	var c redis.UniversalClient
	if viper.GetBool("cluster") {
		addrs := strings.Split(viper.GetString("cluster_addrs"), ",")
		c = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:     addrs,
			Password:  viper.GetString("password"),
			TLSConfig: getTLSConfig(),
		})
	} else {
		c = redis.NewClient(&redis.Options{
			Addr:      viper.GetString("uri"),
			DB:        viper.GetInt("db"),
			Password:  viper.GetString("password"),
			TLSConfig: getTLSConfig(),
		})
	}
	return rdb.NewRDB(c)
}

// createRDB creates a Inspector instance using flag values and returns it.
func createInspector() *asynq.Inspector {
	return asynq.NewInspector(getRedisConnOpt())
}

func getRedisConnOpt() asynq.RedisConnOpt {
	if viper.GetBool("cluster") {
		addrs := strings.Split(viper.GetString("cluster_addrs"), ",")
		return asynq.RedisClusterClientOpt{
			Addrs:     addrs,
			Password:  viper.GetString("password"),
			TLSConfig: getTLSConfig(),
		}
	}
	return asynq.RedisClientOpt{
		Addr:      viper.GetString("uri"),
		DB:        viper.GetInt("db"),
		Password:  viper.GetString("password"),
		TLSConfig: getTLSConfig(),
	}
}

func getTLSConfig() *tls.Config {
	tlsServer := viper.GetString("tls_server")
	if tlsServer == "" {
		return nil
	}
	return &tls.Config{ServerName: tlsServer}
}

// printTable is a helper function to print data in table format.
//
// cols is a list of headers and printRow specifies how to print rows.
//
// Example:
// type User struct {
//     Name string
//     Addr string
//     Age  int
// }
// data := []*User{{"user1", "addr1", 24}, {"user2", "addr2", 42}, ...}
// cols := []string{"Name", "Addr", "Age"}
// printRows := func(w io.Writer, tmpl string) {
//     for _, u := range data {
//         fmt.Fprintf(w, tmpl, u.Name, u.Addr, u.Age)
//     }
// }
// printTable(cols, printRows)
func printTable(cols []string, printRows func(w io.Writer, tmpl string)) {
	format := strings.Repeat("%v\t", len(cols)) + "\n"
	tw := new(tabwriter.Writer).Init(os.Stdout, 0, 8, 2, ' ', 0)
	var headers []interface{}
	var seps []interface{}
	for _, name := range cols {
		headers = append(headers, name)
		seps = append(seps, strings.Repeat("-", len(name)))
	}
	fmt.Fprintf(tw, format, headers...)
	fmt.Fprintf(tw, format, seps...)
	printRows(tw, format)
	tw.Flush()
}

// sprintBytes returns a string representation of the given byte slice if data is printable.
// If data is not printable, it returns a string describing it is not printable.
func sprintBytes(payload []byte) string {
	if !isPrintable(payload) {
		return "non-printable bytes"
	}
	return string(payload)
}

func isPrintable(data []byte) bool {
	if !utf8.Valid(data) {
		return false
	}
	isAllSpace := true
	for _, r := range string(data) {
		if !unicode.IsPrint(r) {
			return false
		}
		if !unicode.IsSpace(r) {
			isAllSpace = false
		}
	}
	return !isAllSpace
}

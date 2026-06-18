package cmd

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cedar-graph/cedargraph-cli/internal/client"
	"github.com/cedar-graph/cedargraph-cli/internal/display"
	"github.com/chzyer/readline"
	"github.com/spf13/cobra"
)

var shellCmd = &cobra.Command{
	Use:   "shell",
	Short: "Start interactive Cypher shell",
	Long:  "Connect to GraphD and start an interactive Cypher REPL",
	RunE: func(cmd *cobra.Command, args []string) error {
		c, err := client.NewGraphClient(graphdAddr)
		if err != nil {
			return fmt.Errorf("connect to graphd: %w", err)
		}
		defer c.Close()

		rl, err := readline.NewEx(&readline.Config{
			Prompt:       "\033[36mcedar>\033[0m ",
			HistoryFile:  "/tmp/.cedargraph_history",
			AutoComplete: nil,
		})
		if err != nil {
			return fmt.Errorf("init readline: %w", err)
		}
		defer rl.Close()

		shell := &ShellSession{
			client:  c,
			rl:      rl,
			format:  display.FormatTable,
			timeout: shellTimeout,
			verbose: shellVerbose,
		}

		fmt.Println()
		fmt.Println("\033[1mCedarGraph Interactive Shell\033[0m")
		fmt.Printf("Connected to %s\n", graphdAddr)
		fmt.Println("Type 'help' for commands, 'quit' to exit")
		fmt.Println()

		return shell.Run()
	},
}

type ShellSession struct {
	client  *client.GraphClient
	rl      *readline.Instance
	format  display.OutputFormat
	timeout time.Duration
	verbose bool
}

func (s *ShellSession) Run() error {
	for {
		line, err := s.rl.Readline()
		if err != nil {
			break
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		// Handle multi-line input (accumulate until semicolon)
		query := line
		for !strings.HasSuffix(query, ";") && !strings.HasPrefix(strings.ToLower(query), ":") {
			s.rl.SetPrompt("\033[36m  ..>\033[0m ")
			line, err = s.rl.Readline()
			if err != nil {
				break
			}
			query += " " + strings.TrimSpace(line)
		}
		s.rl.SetPrompt("\033[36mcedar>\033[0m ")

		query = strings.TrimRight(query, ";")
		query = strings.TrimSpace(query)
		if query == "" {
			continue
		}

		if s.handleCommand(query) {
			continue
		}

		s.executeQuery(query)
	}
	return nil
}

func (s *ShellSession) handleCommand(cmd string) bool {
	lower := strings.ToLower(cmd)

	switch {
	case lower == "quit" || lower == "exit" || lower == ":quit" || lower == ":q":
		fmt.Println("Disconnected.")
		return true

	case lower == "help" || lower == ":help" || lower == ":h" || lower == "?":
		printShellHelp()
		return true

	case lower == "clear" || lower == ":clear":
		fmt.Print("\033[2J\033[H")
		return true

	case strings.HasPrefix(lower, ":format"):
		parts := strings.Fields(cmd)
		if len(parts) < 2 {
			fmt.Printf("Current format: %s\n", s.format)
			fmt.Println("Available: table, json, csv")
			return true
		}
		switch parts[1] {
		case "table", "json", "csv":
			s.format = display.OutputFormat(parts[1])
			fmt.Printf("Output format: %s\n", s.format)
		default:
			fmt.Println("Unknown format. Available: table, json, csv")
		}
		return true

	case lower == ":status":
		s.showStatus()
		return true

	case lower == ":timing":
		fmt.Println("Timing is always shown in table mode")
		return true

	case strings.HasPrefix(lower, ":timeout"):
		parts := strings.Fields(cmd)
		if len(parts) < 2 {
			fmt.Printf("Current timeout: %v\n", s.timeout)
			return true
		}
		d, err := time.ParseDuration(parts[1])
		if err != nil {
			fmt.Printf("Invalid duration: %s (e.g. 5s, 1m, 30s)\n", parts[1])
			return true
		}
		s.timeout = d
		fmt.Printf("Timeout: %v\n", s.timeout)
		return true

	case lower == ":verbose":
		s.verbose = !s.verbose
		fmt.Printf("Verbose: %v\n", s.verbose)
		return true
	}

	return false
}

func (s *ShellSession) executeQuery(query string) {
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()

	start := time.Now()
	result, err := s.client.ExecuteQuery(ctx, query)
	elapsed := time.Since(start)

	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			fmt.Printf("\033[31mError: query timeout after %v\033[0m\n", s.timeout)
		} else {
			fmt.Printf("\033[31mError: %v\033[0m\n", err)
		}
		return
	}

	if result.Error != "" {
		fmt.Printf("\033[31m%s\033[0m\n", result.Error)
		return
	}

	if len(result.Columns) > 0 {
		display.PrintResult(result.Columns, result.Rows, s.format)
	} else if s.format != display.FormatJSON {
		fmt.Printf("OK (%d rows)\n", len(result.Rows))
	}

	if s.format == display.FormatTable {
		serverTime := result.Stats.ExecutionTimeUs
		if s.verbose && serverTime > 0 {
			networkOverhead := elapsed.Microseconds() - int64(serverTime)
			fmt.Printf("  Time: %.2fms (server: %.2fms, network: %.2fms) | Rows: %d\n",
				float64(elapsed.Microseconds())/1000.0,
				float64(serverTime)/1000.0,
				float64(networkOverhead)/1000.0,
				len(result.Rows))
		} else {
			fmt.Printf("  Time: %.2fms | Rows: %d\n",
				float64(elapsed.Microseconds())/1000.0,
				len(result.Rows))
		}
	}
}

func (s *ShellSession) showStatus() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result, err := s.client.ExecuteQuery(ctx, "SHOW PARTITIONS")
	if err != nil {
		fmt.Printf("\033[31mError: %v\033[0m\n", err)
		return
	}

	if result.Error != "" {
		fmt.Printf("\033[31m%s\033[0m\n", result.Error)
		return
	}

	if len(result.Columns) > 0 {
		display.PrintResult(result.Columns, result.Rows, s.format)
	}
}

func printShellHelp() {
	fmt.Print(`
Commands:
  help, :h, ?       Show this help
  quit, :q          Exit shell
  clear, :clear     Clear screen
  :format [fmt]     Set output format (table, json, csv)
  :timeout [dur]    Set query timeout (e.g. 5s, 1m)
  :verbose          Toggle verbose timing
  :status           Show cluster status

Cypher:
  Enter any Cypher statement followed by semicolon or newline
  Multi-line queries are supported (end with ;)

Examples:
  MATCH (n) RETURN n LIMIT 10;
  CREATE (n:Person {name: 'Alice', age: 30});
  MATCH (n:Person) WHERE n.name = 'Alice' RETURN n;

`)
}

func printShellResult(result *client.QueryResult) {
	if len(result.Columns) == 0 {
		fmt.Println("(no columns)")
		return
	}

	widths := make([]int, len(result.Columns))
	for i, col := range result.Columns {
		widths[i] = len(col)
	}
	for _, row := range result.Rows {
		for i, cell := range row {
			if i < len(widths) && len(cell) > widths[i] {
				widths[i] = len(cell)
			}
		}
	}

	sep := "  "
	for i, w := range widths {
		if i > 0 {
			sep += " │ "
		}
		sep += strings.Repeat("─", w+2)
	}

	fmt.Println(sep)
	header := "  "
	for i, col := range result.Columns {
		if i > 0 {
			header += " │ "
		}
		header += fmt.Sprintf("%-*s", widths[i], col)
	}
	fmt.Println(header)
	fmt.Println(sep)

	for _, row := range result.Rows {
		line := "  "
		for i, cell := range row {
			if i > 0 {
				line += " │ "
			}
			if i < len(widths) {
				line += fmt.Sprintf("%-*s", widths[i], cell)
			}
		}
		fmt.Println(line)
	}
	fmt.Println(sep)
	fmt.Printf("  %d row(s)\n", len(result.Rows))
}

var (
	shellTimeout time.Duration
	shellVerbose bool
)

func init() {
	rootCmd.AddCommand(shellCmd)
	shellCmd.Flags().StringVar(&graphdAddr, "graphd", "127.0.0.1:9669", "GraphD address")
	shellCmd.Flags().DurationVarP(&shellTimeout, "timeout", "t", 60*time.Second, "Query timeout")
	shellCmd.Flags().BoolVarP(&shellVerbose, "verbose", "v", false, "Show detailed timing")
}

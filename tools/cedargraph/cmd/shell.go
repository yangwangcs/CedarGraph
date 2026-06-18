package cmd

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cedar-graph/cedargraph-cli/internal/client"
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

		fmt.Println()
		fmt.Println("\033[1mCedarGraph Interactive Shell\033[0m")
		fmt.Printf("Connected to %s\n", graphdAddr)
		fmt.Println("Type Cypher queries, 'help' for commands, 'quit' to exit")
		fmt.Println()

		for {
			line, err := rl.Readline()
			if err != nil {
				break
			}

			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}

			switch strings.ToLower(line) {
			case "quit", "exit", ":quit", ":q":
				fmt.Println("Bye!")
				return nil
			case "help", ":help", ":h":
				printShellHelp()
				continue
			case "clear", ":clear":
				fmt.Print("\033[2J\033[H")
				continue
			}

			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			result, err := c.ExecuteQuery(ctx, line)
			cancel()

			if err != nil {
				fmt.Printf("\033[31mError: %v\033[0m\n", err)
				continue
			}

			if result.Error != "" {
				fmt.Printf("\033[31m%s\033[0m\n", result.Error)
				continue
			}

			if len(result.Columns) > 0 {
				printShellResult(result)
			} else {
				fmt.Printf("OK (%d rows)\n", len(result.Rows))
			}
		}
		return nil
	},
}

func printShellHelp() {
	fmt.Print(`
Commands:
  help, :h          Show this help
  quit, :q          Exit shell
  clear, :clear     Clear screen

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

func init() {
	rootCmd.AddCommand(shellCmd)
	shellCmd.Flags().StringVar(&graphdAddr, "graphd", "127.0.0.1:9669", "GraphD address")
}

package cmd

import (
	"context"
	"fmt"
	"time"

	"github.com/cedar-graph/cedargraph-cli/internal/client"
	"github.com/cedar-graph/cedargraph-cli/internal/display"
	"github.com/spf13/cobra"
)

var (
	graphdAddr string
	outputFmt  string
	queryTimeout time.Duration
	verbose    bool
)

var queryCmd = &cobra.Command{
	Use:   "query [cypher statement]",
	Short: "Execute a Cypher query",
	Long:  "Execute a single Cypher query against GraphD and print results",
	Args:  cobra.MinimumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		query := args[0]

		// Connect with retry
		c, err := client.NewGraphClient(graphdAddr)
		if err != nil {
			return fmt.Errorf("connect to graphd: %w", err)
		}
		defer c.Close()

		// Create context with timeout
		ctx, cancel := context.WithTimeout(context.Background(), queryTimeout)
		defer cancel()

		// Execute with client-side timing
		clientStart := time.Now()
		result, err := c.ExecuteQuery(ctx, query)
		clientElapsed := time.Since(clientStart)

		if err != nil {
			if ctx.Err() == context.DeadlineExceeded {
				return fmt.Errorf("query timeout after %v", queryTimeout)
			}
			return fmt.Errorf("query failed: %w", err)
		}

		if result.Error != "" {
			return fmt.Errorf("query error: %s", result.Error)
		}

		format := display.OutputFormat(outputFmt)
		if len(result.Columns) > 0 {
			display.PrintResult(result.Columns, result.Rows, format)
		} else if format != display.FormatJSON {
			fmt.Println("OK")
		}

		// Print timing statistics
		printTiming(result, clientElapsed, format)

		return nil
	},
}

func printTiming(result *client.QueryResult, clientElapsed time.Duration, format display.OutputFormat) {
	if format == display.FormatJSON || format == display.FormatCSV {
		return
	}

	serverTime := result.Stats.ExecutionTimeUs
	networkOverhead := clientElapsed.Microseconds() - int64(serverTime)

	fmt.Println()
	if verbose {
		fmt.Printf("  ┌─ Timing ─────────────────────────────\n")
		fmt.Printf("  │ Client total:   %12.2f ms\n", float64(clientElapsed.Microseconds())/1000.0)
		if serverTime > 0 {
			fmt.Printf("  │ Server execute: %12.2f ms\n", float64(serverTime)/1000.0)
			fmt.Printf("  │ Network+other:  %12.2f ms\n", float64(networkOverhead)/1000.0)
		}
		if result.Stats != nil {
			fmt.Printf("  │ Rows scanned:   %12d\n", result.Stats.RowsScanned)
			fmt.Printf("  │ Rows returned:  %12d\n", result.Stats.RowsReturned)
			fmt.Printf("  │ Storage nodes:  %12d\n", result.Stats.StorageNodesUsed)
		}
		fmt.Printf("  └──────────────────────────────────────\n")
	} else {
		if result.Stats != nil && serverTime > 0 {
			fmt.Printf("  Time: %.2fms (server: %.2fms, network: %.2fms) | Rows: %d\n",
				float64(clientElapsed.Microseconds())/1000.0,
				float64(serverTime)/1000.0,
				float64(networkOverhead)/1000.0,
				result.Stats.RowsReturned)
		} else {
			fmt.Printf("  Time: %.2fms | Rows: %d\n",
				float64(clientElapsed.Microseconds())/1000.0,
				len(result.Rows))
		}
	}
}

func init() {
	rootCmd.AddCommand(queryCmd)
	queryCmd.Flags().StringVar(&graphdAddr, "graphd", "127.0.0.1:9669", "GraphD address")
	queryCmd.Flags().StringVarP(&outputFmt, "format", "f", "table", "Output format: table, json, csv")
	queryCmd.Flags().DurationVarP(&queryTimeout, "timeout", "t", 30*time.Second, "Query timeout (e.g. 5s, 1m, 30s)")
	queryCmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "Show detailed timing breakdown")
}

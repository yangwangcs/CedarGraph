package cmd

import (
	"context"
	"fmt"
	"time"

	"github.com/cedar-graph/cedargraph-cli/internal/client"
	"github.com/spf13/cobra"
)

var graphdAddr string

var queryCmd = &cobra.Command{
	Use:   "query [cypher statement]",
	Short: "Execute a Cypher query",
	Long:  "Execute a single Cypher query against GraphD and print results",
	Args:  cobra.MinimumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		query := args[0]

		c, err := client.NewGraphClient(graphdAddr)
		if err != nil {
			return fmt.Errorf("connect to graphd: %w", err)
		}
		defer c.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		result, err := c.ExecuteQuery(ctx, query)
		if err != nil {
			return fmt.Errorf("query failed: %w", err)
		}

		if result.Error != "" {
			return fmt.Errorf("query error: %s", result.Error)
		}

		fmt.Printf("  %d row(s) returned\n", len(result.Rows))
		return nil
	},
}

func init() {
	rootCmd.AddCommand(queryCmd)
	queryCmd.Flags().StringVar(&graphdAddr, "graphd", "127.0.0.1:9669", "GraphD address")
}

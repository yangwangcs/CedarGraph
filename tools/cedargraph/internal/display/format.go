package display

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

type OutputFormat string

const (
	FormatTable OutputFormat = "table"
	FormatJSON  OutputFormat = "json"
	FormatCSV   OutputFormat = "csv"
)

func PrintResult(columns []string, rows [][]string, format OutputFormat) {
	switch format {
	case FormatJSON:
		printJSON(columns, rows)
	case FormatCSV:
		printCSV(columns, rows)
	default:
		PrintQueryResult(columns, rows)
	}
}

func printJSON(columns []string, rows [][]string) {
	var result []map[string]string
	for _, row := range rows {
		record := make(map[string]string)
		for i, col := range columns {
			if i < len(row) {
				record[col] = row[i]
			}
		}
		result = append(result, record)
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	enc.Encode(result)
}

func printCSV(columns []string, rows [][]string) {
	// Header
	fmt.Println(strings.Join(columns, ","))
	// Rows
	for _, row := range rows {
		var escaped []string
		for _, cell := range row {
			if strings.Contains(cell, ",") || strings.Contains(cell, "\"") {
				cell = "\"" + strings.ReplaceAll(cell, "\"", "\"\"") + "\""
			}
			escaped = append(escaped, cell)
		}
		fmt.Println(strings.Join(escaped, ","))
	}
}

package display

import (
	"fmt"
	"strings"
)

type NodeStatus struct {
	Name    string
	Address string
	Port    int
	Role    string
	Status  string
	PID     int
}

func PrintClusterStatus(nodes []NodeStatus) {
	nameW, addrW, roleW, statusW := 10, 20, 8, 10
	header := fmt.Sprintf("  %-*s │ %-*s │ %-5s │ %-*s │ %-*s",
		nameW, "Service", addrW, "Address", "Port", roleW, "Role", statusW, "Status")
	sep := "  " + strings.Repeat("─", nameW+1) + "┼" + strings.Repeat("─", addrW+2) +
		"┼" + strings.Repeat("─", 7) + "┼" + strings.Repeat("─", roleW+2) +
		"┼" + strings.Repeat("─", statusW+2)

	fmt.Println(sep)
	fmt.Println(header)
	fmt.Println(sep)

	for _, n := range nodes {
		status := n.Status
		switch status {
		case "online":
			status = "\033[32m● " + status + "\033[0m"
		case "stopped":
			status = "\033[31m● " + status + "\033[0m"
		default:
			status = "\033[33m● " + status + "\033[0m"
		}
		fmt.Printf("  %-*s │ %-*s │ %-5d │ %-*s │ %s\n",
			nameW, n.Name, addrW, fmt.Sprintf("%s:%d", n.Address, n.Port),
			n.Port, roleW, n.Role, status)
	}
	fmt.Println(sep)
}

func PrintPartitionSummary(partitionCount int, replicaFactor int, totalSize string) {
	fmt.Printf("\n  Partitions: %d │ Replicas: %d │ Storage: %s\n\n",
		partitionCount, replicaFactor, totalSize)
}

func PrintQueryResult(columns []string, rows [][]string) {
	if len(columns) == 0 {
		fmt.Println("(no results)")
		return
	}

	widths := make([]int, len(columns))
	for i, col := range columns {
		widths[i] = len(col)
	}
	for _, row := range rows {
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

	header := "  "
	for i, col := range columns {
		if i > 0 {
			header += " │ "
		}
		header += fmt.Sprintf("%-*s", widths[i], col)
	}

	fmt.Println(sep)
	fmt.Println(header)
	fmt.Println(sep)

	for _, row := range rows {
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
	fmt.Printf("  %d row(s)\n", len(rows))
}

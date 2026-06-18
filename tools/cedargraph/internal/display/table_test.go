package display

import (
	"bytes"
	"os"
	"strings"
	"testing"
)

func TestPrintQueryResult(t *testing.T) {
	// Capture stdout
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	columns := []string{"name", "age"}
	rows := [][]string{
		{"Alice", "30"},
		{"Bob", "25"},
	}

	PrintQueryResult(columns, rows)

	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	buf.ReadFrom(r)
	output := buf.String()

	if !strings.Contains(output, "name") {
		t.Error("output missing column 'name'")
	}
	if !strings.Contains(output, "age") {
		t.Error("output missing column 'age'")
	}
	if !strings.Contains(output, "Alice") {
		t.Error("output missing value 'Alice'")
	}
	if !strings.Contains(output, "Bob") {
		t.Error("output missing value 'Bob'")
	}
	if !strings.Contains(output, "2 row(s)") {
		t.Error("output missing row count")
	}
}

func TestPrintQueryResultEmpty(t *testing.T) {
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	PrintQueryResult([]string{}, [][]string{})

	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	buf.ReadFrom(r)
	output := buf.String()

	if !strings.Contains(output, "no results") {
		t.Error("expected 'no results' for empty columns")
	}
}

func TestPrintClusterStatus(t *testing.T) {
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	nodes := []NodeStatus{
		{Name: "MetaD", Address: "127.0.0.1", Port: 9559, Role: "leader", Status: "online"},
		{Name: "StorageD", Address: "127.0.0.1", Port: 9779, Role: "storage", Status: "stopped"},
	}

	PrintClusterStatus(nodes)

	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	buf.ReadFrom(r)
	output := buf.String()

	if !strings.Contains(output, "MetaD") {
		t.Error("output missing MetaD")
	}
	if !strings.Contains(output, "StorageD") {
		t.Error("output missing StorageD")
	}
	if !strings.Contains(output, "9559") {
		t.Error("output missing port 9559")
	}
}

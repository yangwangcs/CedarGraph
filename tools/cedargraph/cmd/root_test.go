package cmd

import (
	"bytes"
	"strings"
	"testing"
)

func executeCommand(args ...string) (string, error) {
	buf := new(bytes.Buffer)
	rootCmd.SetOut(buf)
	rootCmd.SetErr(buf)
	rootCmd.SetArgs(args)
	err := rootCmd.Execute()
	return buf.String(), err
}

func TestQueryCommandHelp(t *testing.T) {
	output, err := executeCommand("query", "--help")
	if err != nil {
		t.Fatalf("query --help failed: %v", err)
	}
	if !strings.Contains(output, "Cypher") {
		t.Error("help output missing 'Cypher'")
	}
	if !strings.Contains(output, "--graphd") {
		t.Error("help output missing '--graphd' flag")
	}
}

func TestShellCommandHelp(t *testing.T) {
	output, err := executeCommand("shell", "--help")
	if err != nil {
		t.Fatalf("shell --help failed: %v", err)
	}
	if !strings.Contains(output, "interactive") && !strings.Contains(output, "Interactive") {
		t.Error("help output missing 'interactive'")
	}
}

func TestStatusCommandHelp(t *testing.T) {
	output, err := executeCommand("status", "--help")
	if err != nil {
		t.Fatalf("status --help failed: %v", err)
	}
	if !strings.Contains(output, "cluster") && !strings.Contains(output, "Cluster") {
		t.Error("help output missing 'cluster'")
	}
}

func TestStartCommandHelp(t *testing.T) {
	output, err := executeCommand("start", "--help")
	if err != nil {
		t.Fatalf("start --help failed: %v", err)
	}
	if !strings.Contains(output, "Start") {
		t.Error("help output missing 'Start'")
	}
}

func TestStopCommandHelp(t *testing.T) {
	output, err := executeCommand("stop", "--help")
	if err != nil {
		t.Fatalf("stop --help failed: %v", err)
	}
	if !strings.Contains(output, "Stop") && !strings.Contains(output, "stop") {
		t.Error("help output missing 'stop'")
	}
}

func TestRootCommandHelp(t *testing.T) {
	output, err := executeCommand("--help")
	if err != nil {
		t.Fatalf("--help failed: %v", err)
	}
	if !strings.Contains(output, "cedargraph") {
		t.Error("help output missing 'cedargraph'")
	}
	if !strings.Contains(output, "start") {
		t.Error("help output missing 'start' command")
	}
	if !strings.Contains(output, "query") {
		t.Error("help output missing 'query' command")
	}
	if !strings.Contains(output, "shell") {
		t.Error("help output missing 'shell' command")
	}
}

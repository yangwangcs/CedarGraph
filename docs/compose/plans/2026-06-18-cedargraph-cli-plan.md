# CedarGraph CLI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use compose:subagent (recommended) or compose:execute to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Go CLI tool `cedargraph` for cluster management (start/stop/status) and Cypher query execution (query/shell).

**Architecture:** Go binary using cobra for CLI framework, gRPC for communicating with MetaD/StorageD/GraphD. Cluster management spawns native processes. Query client connects to GraphD gRPC service.

**Tech Stack:** Go 1.21+, cobra (CLI), grpc-go (gRPC), tablewriter (output), readline (shell REPL)

---

## [S1] Project Setup

### Task 1: Initialize Go module and project structure

**Files:**
- Create: `tools/cedargraph/go.mod`
- Create: `tools/cedargraph/main.go`
- Create: `tools/cedargraph/cmd/root.go`

- [ ] **Step 1: Install Go (if not installed)**

```bash
brew install go
go version  # should show go1.21+
```

- [ ] **Step 2: Initialize Go module**

```bash
mkdir -p tools/cedargraph
cd tools/cedargraph
go mod init github.com/cedar-graph/cedargraph-cli
```

- [ ] **Step 3: Install dependencies**

```bash
go get github.com/spf13/cobra@latest
go get google.golang.org/grpc@latest
google.golang.org/protobuf@latest
go get github.com/olekukonez/tablewriter@latest
go get github.com/chzyer/readline@latest
```

- [ ] **Step 4: Create main.go**

```go
package main

import (
    "os"
    "github.com/cedar-graph/cedargraph-cli/cmd"
)

func main() {
    if err := cmd.Execute(); err != nil {
        os.Exit(1)
    }
}
```

- [ ] **Step 5: Create cmd/root.go**

```go
package cmd

import (
    "fmt"
    "github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
    Use:   "cedargraph",
    Short: "CedarGraph distributed graph database CLI",
    Long:  "CedarGraph CLI - cluster management and query client for CedarGraph",
}

func Execute() error {
    return rootCmd.Execute()
}
```

- [ ] **Step 6: Verify build**

```bash
cd tools/cedargraph
go build -o cedargraph .
./cedargraph --help
```

Expected: Shows help text with "CedarGraph distributed graph database CLI"

- [ ] **Step 7: Commit**

```bash
git add tools/cedargraph/
git commit -m "feat(cli): initialize Go module with cobra"
```

---

## [S2] Generate gRPC stubs

### Task 2: Generate Go gRPC code from proto files

**Files:**
- Create: `tools/cedargraph/proto/meta_service.pb.go`
- Create: `tools/cedargraph/proto/meta_service_grpc.pb.go`
- Create: `tools/cedargraph/proto/storage_service.pb.go`
- Create: `tools/cedargraph/proto/storage_service_grpc.pb.go`

- [ ] **Step 1: Install protoc Go plugins**

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```

- [ ] **Step 2: Copy proto files and generate**

```bash
cd tools/cedargraph
mkdir -p proto
cp ../../proto/meta_service.proto proto/
cp ../../proto/storage_service.proto proto/
# Copy any dependencies (empty.proto, etc.)
protoc --go_out=. --go_opt=paths=source_relative \
       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
       proto/*.proto
```

- [ ] **Step 3: Verify generated code compiles**

```bash
go build ./...
```

- [ ] **Step 4: Commit**

```bash
git add tools/cedargraph/proto/
git commit -m "feat(cli): generate gRPC stubs from proto"
```

---

## [S3] Cluster Manager

### Task 3: Implement process manager for start/stop

**Files:**
- Create: `tools/cedargraph/internal/cluster/manager.go`
- Create: `tools/cedargraph/internal/cluster/config.go`
- Create: `tools/cedargraph/cmd/start.go`
- Create: `tools/cedargraph/cmd/stop.go`

- [ ] **Step 1: Create config.go — parse cedar.yaml**

```go
package cluster

import (
    "os"
    "gopkg.in/yaml.v3"
)

type ClusterConfig struct {
    MetaD    NodeConfig `yaml:"metad"`
    StorageD NodeConfig `yaml:"storaged"`
    GraphD   NodeConfig `yaml:"graphd"`
}

type NodeConfig struct {
    BindAddress string `yaml:"bind_address"`
    Port        int    `yaml:"port"`
    DataDir     string `yaml:"data_dir"`
    ConfigFile  string `yaml:"config_file"`
}

func LoadConfig(path string) (*ClusterConfig, error) {
    data, err := os.ReadFile(path)
    if err != nil {
        return nil, err
    }
    var config ClusterConfig
    if err := yaml.Unmarshal(data, &config); err != nil {
        return nil, err
    }
    return &config, nil
}
```

- [ ] **Step 2: Create manager.go — process lifecycle**

```go
package cluster

import (
    "fmt"
    "os"
    "os/exec"
    "path/filepath"
    "syscall"
    "time"
)

type Manager struct {
    config   *ClusterConfig
    baseDir  string
    pidDir   string
}

func NewManager(config *ClusterConfig, baseDir string) *Manager {
    pidDir := filepath.Join(baseDir, ".cedargraph", "pids")
    os.MkdirAll(pidDir, 0755)
    return &Manager{config: config, baseDir: baseDir, pidDir: pidDir}
}

type NodeInfo struct {
    Name    string
    PID     int
    Address string
    Port    int
    Status  string
}

func (m *Manager) StartMetaD() (*NodeInfo, error) {
    return m.startNode("metad", m.config.MetaD)
}

func (m *Manager) StartStorageD() (*NodeInfo, error) {
    return m.startNode("storaged", m.config.StorageD)
}

func (m *Manager) StartGraphD() (*NodeInfo, error) {
    return m.startNode("graphd", m.config.GraphD)
}

func (m *Manager) startNode(name string, cfg NodeConfig) (*NodeInfo, error) {
    binary := filepath.Join(m.baseDir, "build", "cedar-"+name)
    args := []string{"--config", cfg.ConfigFile}
    
    cmd := exec.Command(binary, args...)
    cmd.Stdout = os.Stdout
    cmd.Stderr = os.Stderr
    
    if err := cmd.Start(); err != nil {
        return nil, fmt.Errorf("failed to start %s: %w", name, err)
    }
    
    // Save PID
    pidFile := filepath.Join(m.pidDir, name+".pid")
    os.WriteFile(pidFile, []byte(fmt.Sprintf("%d", cmd.Process.Pid)), 0644)
    
    return &NodeInfo{
        Name:    name,
        PID:     cmd.Process.Pid,
        Address: cfg.BindAddress,
        Port:    cfg.Port,
        Status:  "started",
    }, nil
}

func (m *Manager) StopAll() error {
    for _, name := range []string{"graphd", "storaged", "metad"} {
        m.stopNode(name)
    }
    return nil
}

func (m *Manager) stopNode(name string) error {
    pidFile := filepath.Join(m.pidDir, name+".pid")
    data, err := os.ReadFile(pidFile)
    if err != nil {
        return nil // not running
    }
    var pid int
    fmt.Sscanf(string(data), "%d", &pid)
    
    process, err := os.FindProcess(pid)
    if err != nil {
        return nil
    }
    
    process.Signal(syscall.SIGTERM)
    os.Remove(pidFile)
    return nil
}
```

- [ ] **Step 3: Create cmd/start.go**

```go
package cmd

import (
    "fmt"
    "os"
    "time"
    "github.com/spf13/cobra"
    "github.com/cedar-graph/cedargraph-cli/internal/cluster"
)

var startCmd = &cobra.Command{
    Use:   "start",
    Short: "Start CedarGraph cluster",
    RunE: func(cmd *cobra.Command, args []string) error {
        configPath, _ := cmd.Flags().GetString("config")
        config, err := cluster.LoadConfig(configPath)
        if err != nil {
            return fmt.Errorf("failed to load config: %w", err)
        }
        
        mgr := cluster.NewManager(config, ".")
        
        fmt.Println("Starting CedarGraph cluster...")
        fmt.Println()
        
        // Start MetaD
        meta, err := mgr.StartMetaD()
        if err != nil {
            return err
        }
        fmt.Printf("  MetaD    │ PID %d │ %s:%d │ Started\n", meta.PID, meta.Address, meta.Port)
        time.Sleep(2 * time.Second) // Wait for leader election
        
        // Start StorageD
        storage, err := mgr.StartStorageD()
        if err != nil {
            return err
        }
        fmt.Printf("  StorageD │ PID %d │ %s:%d │ Started\n", storage.PID, storage.Address, storage.Port)
        time.Sleep(1 * time.Second)
        
        // Start GraphD
        graph, err := mgr.StartGraphD()
        if err != nil {
            return err
        }
        fmt.Printf("  GraphD   │ PID %d │ %s:%d │ Started\n", graph.PID, graph.Address, graph.Port)
        
        fmt.Println()
        fmt.Println("Cluster ready. Use 'cedargraph status' to check health.")
        return nil
    },
}

func init() {
    rootCmd.AddCommand(startCmd)
    startCmd.Flags().StringP("config", "c", "config/cedar.yaml", "Config file path")
}
```

- [ ] **Step 4: Create cmd/stop.go**

```go
package cmd

import (
    "fmt"
    "github.com/spf13/cobra"
    "github.com/cedar-graph/cedargraph-cli/internal/cluster"
)

var stopCmd = &cobra.Command{
    Use:   "stop",
    Short: "Stop CedarGraph cluster",
    RunE: func(cmd *cobra.Command, args []string) error {
        configPath, _ := cmd.Flags().GetString("config")
        config, err := cluster.LoadConfig(configPath)
        if err != nil {
            return err
        }
        
        mgr := cluster.NewManager(config, ".")
        fmt.Println("Stopping CedarGraph cluster...")
        mgr.StopAll()
        fmt.Println("Cluster stopped.")
        return nil
    },
}

func init() {
    rootCmd.AddCommand(stopCmd)
    stopCmd.Flags().StringP("config", "c", "config/cedar.yaml", "Config file path")
}
```

- [ ] **Step 5: Test build**

```bash
cd tools/cedargraph
go build -o cedargraph .
./cedargraph start --help
./cedargraph stop --help
```

- [ ] **Step 6: Commit**

```bash
git add tools/cedargraph/
git commit -m "feat(cli): add start/stop cluster commands"
```

---

## [S4] Cluster Status

### Task 4: Implement status command via gRPC

**Files:**
- Create: `tools/cedargraph/internal/client/connection.go`
- Create: `tools/cedargraph/internal/display/table.go`
- Create: `tools/cedargraph/cmd/status.go`

- [ ] **Step 1: Create connection.go — gRPC client wrapper**

```go
package client

import (
    "context"
    "time"
    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
    pb "github.com/cedar-graph/cedargraph-cli/proto"
)

type Client struct {
    metaConn    *grpc.ClientConn
    metaClient  pb.MetaServiceClient
}

func NewClient(metaAddr string) (*Client, error) {
    conn, err := grpc.Dial(metaAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
    if err != nil {
        return nil, err
    }
    return &Client{
        metaConn:   conn,
        metaClient: pb.NewMetaServiceClient(conn),
    }, nil
}

func (c *Client) Close() {
    if c.metaConn != nil {
        c.metaConn.Close()
    }
}

func (c *Client) GetAliveNodes(ctx context.Context) ([]*pb.NodeInfo, error) {
    resp, err := c.metaClient.GetAliveNodes(ctx, &pb.GetAliveNodesRequest{})
    if err != nil {
        return nil, err
    }
    return resp.Nodes, nil
}

func (c *Client) GetPartitionMap(ctx context.Context, space string) (*pb.PartitionMap, error) {
    resp, err := c.metaClient.GetSpacePartitionMap(ctx, &pb.GetSpacePartitionMapRequest{
        SpaceName: space,
    })
    if err != nil {
        return nil, err
    }
    return resp.PartitionMap, nil
}
```

- [ ] **Step 2: Create display/table.go — formatted output**

```go
package display

import (
    "fmt"
    "os"
    "github.com/olekukonez/tablewriter"
)

type NodeStatus struct {
    Name    string
    Address string
    Port    int
    Role    string
    Status  string
}

func PrintClusterStatus(nodes []NodeStatus) {
    table := tablewriter.NewWriter(os.Stdout)
    table.SetHeader([]string{"Service", "Address", "Port", "Role", "Status"})
    table.SetBorder(true)
    table.SetCenterSeparator("│")
    table.SetColumnSeparator("│")
    table.SetRowSeparator("─")
    
    for _, n := range nodes {
        status := n.Status
        if status == "Online" {
            status = "\033[32m" + status + "\033[0m"
        } else {
            status = "\033[31m" + status + "\033[0m"
        }
        table.Append([]string{n.Name, n.Address, fmt.Sprintf("%d", n.Port), n.Role, status})
    }
    table.Render()
}
```

- [ ] **Step 3: Create cmd/status.go**

```go
package cmd

import (
    "context"
    "fmt"
    "github.com/spf13/cobra"
    "github.com/cedar-graph/cedargraph-cli/internal/client"
    "github.com/cedar-graph/cedargraph-cli/internal/display"
)

var statusCmd = &cobra.Command{
    Use:   "status",
    Short: "Show CedarGraph cluster status",
    RunE: func(cmd *cobra.Command, args []string) error {
        metaAddr, _ := cmd.Flags().GetString("metad")
        
        c, err := client.NewClient(metaAddr)
        if err != nil {
            return fmt.Errorf("failed to connect to MetaD: %w", err)
        }
        defer c.Close()
        
        ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
        defer cancel()
        
        nodes, err := c.GetAliveNodes(ctx)
        if err != nil {
            return fmt.Errorf("failed to get nodes: %w", err)
        }
        
        var nodeStatuses []display.NodeStatus
        for _, n := range nodes {
            nodeStatuses = append(nodeStatuses, display.NodeStatus{
                Name:    n.Type.String(),
                Address: n.Address,
                Port:    int(n.Port),
                Status:  "Online",
            })
        }
        
        fmt.Println("CedarGraph Cluster Status")
        fmt.Println("═══════════════════════════════════════")
        display.PrintClusterStatus(nodeStatuses)
        return nil
    },
}

func init() {
    rootCmd.AddCommand(statusCmd)
    statusCmd.Flags().String("metad", "127.0.0.1:9559", "MetaD address")
}
```

- [ ] **Step 4: Test build**

```bash
go build -o cedargraph .
./cedargraph status --help
```

- [ ] **Step 5: Commit**

```bash
git add tools/cedargraph/
git commit -m "feat(cli): add status command with gRPC"
```

---

## [S5] Query Client

### Task 5: Implement query command

**Files:**
- Create: `tools/cedargraph/internal/client/query.go`
- Create: `tools/cedargraph/cmd/query.go`

- [ ] **Step 1: Create query.go — execute Cypher via gRPC**

```go
package client

import (
    "context"
    pb "github.com/cedar-graph/cedargraph-cli/proto"
)

func (c *Client) ExecuteQuery(ctx context.Context, query string) (*pb.ExecuteQueryResponse, error) {
    resp, err := c.graphClient.ExecuteQuery(ctx, &pb.ExecuteQueryRequest{
        Query: query,
    })
    if err != nil {
        return nil, err
    }
    return resp, nil
}
```

- [ ] **Step 2: Create cmd/query.go**

```go
package cmd

import (
    "context"
    "fmt"
    "os"
    "github.com/spf13/cobra"
    "github.com/cedar-graph/cedargraph-cli/internal/client"
)

var queryCmd = &cobra.Command{
    Use:   "query [cypher]",
    Short: "Execute a Cypher query",
    Args:  cobra.MinimumNArgs(1),
    RunE: func(cmd *cobra.Command, args []string) error {
        graphAddr, _ := cmd.Flags().GetString("graphd")
        query := args[0]
        
        c, err := client.NewClient(graphAddr)
        if err != nil {
            return err
        }
        defer c.Close()
        
        ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
        defer cancel()
        
        result, err := c.ExecuteQuery(ctx, query)
        if err != nil {
            return fmt.Errorf("query failed: %w", err)
        }
        
        // Print results as table
        printQueryResult(result)
        return nil
    },
}

func init() {
    rootCmd.AddCommand(queryCmd)
    queryCmd.Flags().String("graphd", "127.0.0.1:9669", "GraphD address")
}
```

- [ ] **Step 3: Commit**

```bash
git add tools/cedargraph/
git commit -m "feat(cli): add query command for Cypher execution"
```

---

## [S6] Interactive Shell

### Task 6: Implement REPL shell

**Files:**
- Create: `tools/cedargraph/cmd/shell.go`

- [ ] **Step 1: Create cmd/shell.go**

```go
package cmd

import (
    "context"
    "fmt"
    "os"
    "strings"
    "github.com/chzyer/readline"
    "github.com/spf13/cobra"
    "github.com/cedar-graph/cedargraph-cli/internal/client"
)

var shellCmd = &cobra.Command{
    Use:   "shell",
    Short: "Start interactive Cypher shell",
    RunE: func(cmd *cobra.Command, args []string) error {
        graphAddr, _ := cmd.Flags().GetString("graphd")
        
        c, err := client.NewClient(graphAddr)
        if err != nil {
            return err
        }
        defer c.Close()
        
        rl, err := readline.New("cedar> ")
        if err != nil {
            return err
        }
        defer rl.Close()
        
        fmt.Println("CedarGraph Shell")
        fmt.Printf("Connected to %s\n", graphAddr)
        fmt.Println("Type 'help' for commands, 'quit' to exit")
        fmt.Println()
        
        for {
            line, err := rl.Readline()
            if err != nil { // EOF
                break
            }
            
            line = strings.TrimSpace(line)
            if line == "" {
                continue
            }
            
            switch strings.ToLower(line) {
            case "quit", "exit", ":quit":
                fmt.Println("Bye!")
                return nil
            case "help", ":help":
                printShellHelp()
                continue
            }
            
            ctx := context.Background()
            result, err := c.ExecuteQuery(ctx, line)
            if err != nil {
                fmt.Printf("Error: %v\n", err)
                continue
            }
            
            printQueryResult(result)
        }
        return nil
    },
}

func init() {
    rootCmd.AddCommand(shellCmd)
    shellCmd.Flags().String("graphd", "127.0.0.1:9669", "GraphD address")
}
```

- [ ] **Step 2: Commit**

```bash
git add tools/cedargraph/
git commit -m "feat(cli): add interactive shell with readline"
```

---

## [S7] Testing

### Task 7: Write unit tests

**Files:**
- Create: `tools/cedargraph/internal/cluster/manager_test.go`
- Create: `tools/cedargraph/internal/client/connection_test.go`

- [ ] **Step 1: Write config loading test**

```go
package cluster

import (
    "os"
    "testing"
)

func TestLoadConfig(t *testing.T) {
    content := `
metad:
  bind_address: "127.0.0.1"
  port: 9559
storaged:
  bind_address: "127.0.0.1"
  port: 9779
graphd:
  bind_address: "127.0.0.1"
  port: 9669
`
    tmpFile, _ := os.CreateTemp("", "cedar-*.yaml")
    tmpFile.WriteString(content)
    tmpFile.Close()
    defer os.Remove(tmpFile.Name())
    
    config, err := LoadConfig(tmpFile.Name())
    if err != nil {
        t.Fatalf("LoadConfig failed: %v", err)
    }
    
    if config.MetaD.Port != 9559 {
        t.Errorf("MetaD port = %d, want 9559", config.MetaD.Port)
    }
    if config.StorageD.Port != 9779 {
        t.Errorf("StorageD port = %d, want 9779", config.StorageD.Port)
    }
}
```

- [ ] **Step 2: Run tests**

```bash
cd tools/cedargraph
go test ./... -v
```

- [ ] **Step 3: Commit**

```bash
git add tools/cedargraph/
git commit -m "test(cli): add unit tests for config and cluster manager"
```

---

## Execution Notes

**Prerequisites:**
- Go 1.21+ installed (`brew install go`)
- protoc installed (`brew install protobuf`)
- CedarGraph built (`make -j4`)

**Build:**
```bash
cd tools/cedargraph
go build -o cedargraph .
```

**Usage:**
```bash
./cedargraph start -c config/cedar.yaml
./cedargraph status --metad 127.0.0.1:9559
./cedargraph query "MATCH (n) RETURN n LIMIT 10" --graphd 127.0.0.1:9669
./cedargraph shell --graphd 127.0.0.1:9669
./cedargraph stop
```

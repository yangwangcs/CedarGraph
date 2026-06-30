package cluster

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"time"
)

type Manager struct {
	config  *ClusterConfig
	baseDir string
	pidDir  string
}

type NodeInfo struct {
	Name    string
	PID     int
	Address string
	Port    int
	Status  string
}

func NewManager(config *ClusterConfig, baseDir string) *Manager {
	pidDir := filepath.Join(baseDir, ".cedargraph", "pids")
	os.MkdirAll(pidDir, 0755)
	return &Manager{config: config, baseDir: baseDir, pidDir: pidDir}
}

func (m *Manager) StartMetaD() (*NodeInfo, error) {
	return m.startMetaD()
}

func (m *Manager) StartStorageD() (*NodeInfo, error) {
	return m.startNode("storaged", m.config.StorageD)
}

func (m *Manager) StartGraphD() (*NodeInfo, error) {
	return m.startNode("graphd", m.config.GraphD)
}

func (m *Manager) startMetaD() (*NodeInfo, error) {
	cfg := m.config.MetaD
	binary := filepath.Join(m.baseDir, cfg.BinaryPath)
	if _, err := os.Stat(binary); err != nil {
		return nil, fmt.Errorf("binary not found: %s (run 'make metad' first)", binary)
	}

	args := []string{}
	if cfg.ConfigFile != "" {
		args = append(args, "--config", filepath.Join(m.baseDir, cfg.ConfigFile))
	}
	if cfg.BindAddr != "" {
		args = append(args, "--listen", fmt.Sprintf("%s:%d", cfg.BindAddr, cfg.Port))
	}
	if cfg.GrpcPort > 0 {
		args = append(args, "--grpc_port", strconv.Itoa(cfg.GrpcPort))
	}
	if cfg.DataDir != "" {
		os.MkdirAll(cfg.DataDir, 0755)
		args = append(args, "--data_dir", cfg.DataDir)
	}
	args = append(args, cfg.ExtraArgs...)

	return m.startCommand("metad", binary, args, cfg)
}

func (m *Manager) startNode(name string, cfg NodeConfig) (*NodeInfo, error) {
	binary := filepath.Join(m.baseDir, cfg.BinaryPath)
	if _, err := os.Stat(binary); err != nil {
		return nil, fmt.Errorf("binary not found: %s (run 'make %s' first)", binary, name)
	}

	args := []string{}
	if cfg.ConfigFile != "" {
		args = append(args, "--config", filepath.Join(m.baseDir, cfg.ConfigFile))
	}
	if cfg.BindAddr != "" {
		args = append(args, "--bind", cfg.BindAddr)
	}
	if cfg.Port > 0 {
		args = append(args, "--port", strconv.Itoa(cfg.Port))
	}
	if cfg.DataDir != "" {
		os.MkdirAll(cfg.DataDir, 0755)
		args = append(args, "--data_dir", cfg.DataDir)
	}
	args = append(args, cfg.ExtraArgs...)

	return m.startCommand(name, binary, args, cfg)
}

func (m *Manager) startCommand(name string, binary string, args []string, cfg NodeConfig) (*NodeInfo, error) {
	cmd := exec.Command(binary, args...)
	cmd.Dir = m.baseDir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start %s: %w", name, err)
	}

	pidFile := filepath.Join(m.pidDir, name+".pid")
	os.WriteFile(pidFile, []byte(strconv.Itoa(cmd.Process.Pid)), 0644)

	go cmd.Wait()

	return &NodeInfo{
		Name:    name,
		PID:     cmd.Process.Pid,
		Address: cfg.BindAddr,
		Port:    cfg.Port,
		Status:  "started",
	}, nil
}

func (m *Manager) StopAll() []error {
	var errs []error
	for _, name := range []string{"graphd", "storaged", "metad"} {
		if err := m.stopNode(name); err != nil {
			errs = append(errs, err)
		}
	}
	return errs
}

func (m *Manager) stopNode(name string) error {
	pidFile := filepath.Join(m.pidDir, name+".pid")
	data, err := os.ReadFile(pidFile)
	if err != nil {
		return nil
	}
	pid, _ := strconv.Atoi(string(data))
	if pid == 0 {
		return nil
	}

	process, err := os.FindProcess(pid)
	if err != nil {
		os.Remove(pidFile)
		return nil
	}

	process.Signal(syscall.SIGTERM)
	os.Remove(pidFile)
	return nil
}

func (m *Manager) IsPortOpen(port int) bool {
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

func (m *Manager) WaitForPort(port int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if m.IsPortOpen(port) {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

func (m *Manager) GetNodeStatus(name string, cfg NodeConfig) NodeInfo {
	info := NodeInfo{
		Name:    name,
		Address: cfg.BindAddr,
		Port:    cfg.Port,
	}

	// Check port first (works regardless of how the process was started)
	if m.IsPortOpen(cfg.Port) {
		info.Status = "online"
	} else {
		info.Status = "stopped"
	}

	// Try to get PID from our pid file
	pidFile := filepath.Join(m.pidDir, name+".pid")
	data, err := os.ReadFile(pidFile)
	if err == nil {
		pid, _ := strconv.Atoi(string(data))
		if pid > 0 {
			info.PID = pid
		}
	}

	return info
}

package cluster

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type ClusterConfig struct {
	MetaD      NodeConfig   `yaml:"metad"`
	StorageD   NodeConfig   `yaml:"storaged"`
	GraphD     NodeConfig   `yaml:"graphd"`
	GraphDList []NodeConfig `yaml:"graphd_instances"` // Multiple GraphD instances
}

type NodeConfig struct {
	BinaryPath string `yaml:"binary_path"`
	BindAddr   string `yaml:"bind_address"`
	Port       int    `yaml:"port"`
	GrpcPort   int    `yaml:"grpc_port"`
	DataDir    string `yaml:"data_dir"`
	ConfigFile string `yaml:"config_file"`
	ExtraArgs  []string `yaml:"extra_args"`
}

func LoadConfig(path string) (*ClusterConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var config ClusterConfig
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	config.defaults()
	return &config, nil
}

func (c *ClusterConfig) defaults() {
	if c.MetaD.BindAddr == "" {
		c.MetaD.BindAddr = "127.0.0.1"
	}
	if c.MetaD.Port == 0 {
		c.MetaD.Port = 9559
	}
	if c.MetaD.GrpcPort == 0 {
		c.MetaD.GrpcPort = 10559
	}
	if c.StorageD.BindAddr == "" {
		c.StorageD.BindAddr = "127.0.0.1"
	}
	if c.StorageD.Port == 0 {
		c.StorageD.Port = 9779
	}
	if c.GraphD.BindAddr == "" {
		c.GraphD.BindAddr = "127.0.0.1"
	}
	if c.GraphD.Port == 0 {
		c.GraphD.Port = 9669
	}
	if c.MetaD.BinaryPath == "" {
		c.MetaD.BinaryPath = "build/cedar-metad"
	}
	if c.StorageD.BinaryPath == "" {
		c.StorageD.BinaryPath = "build/cedar-storaged"
	}
	if c.GraphD.BinaryPath == "" {
		c.GraphD.BinaryPath = "build/cedar-graphd"
	}
}

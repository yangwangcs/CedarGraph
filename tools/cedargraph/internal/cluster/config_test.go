package cluster

import (
	"os"
	"testing"
)

func TestLoadConfig(t *testing.T) {
	content := `
metad:
  binary_path: build/cedar-metad
  bind_address: "127.0.0.1"
  port: 9559
  grpc_port: 10559
  data_dir: /tmp/cedar/test/meta
  config_file: config/cedar.yaml
storaged:
  binary_path: build/cedar-storaged
  bind_address: "127.0.0.1"
  port: 9779
  data_dir: /tmp/cedar/test/storage
  config_file: config/cedar.yaml
graphd:
  binary_path: build/cedar-graphd
  bind_address: "127.0.0.1"
  port: 9669
  config_file: config/cedar.yaml
`
	tmpFile, err := os.CreateTemp("", "cedar-test-*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatal(err)
	}
	tmpFile.Close()

	config, err := LoadConfig(tmpFile.Name())
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	if config.MetaD.Port != 9559 {
		t.Errorf("MetaD port = %d, want 9559", config.MetaD.Port)
	}
	if config.MetaD.GrpcPort != 10559 {
		t.Errorf("MetaD grpc_port = %d, want 10559", config.MetaD.GrpcPort)
	}
	if config.StorageD.Port != 9779 {
		t.Errorf("StorageD port = %d, want 9779", config.StorageD.Port)
	}
	if config.GraphD.Port != 9669 {
		t.Errorf("GraphD port = %d, want 9669", config.GraphD.Port)
	}
	if config.MetaD.BindAddr != "127.0.0.1" {
		t.Errorf("MetaD bind = %q, want 127.0.0.1", config.MetaD.BindAddr)
	}
}

func TestLoadConfigDefaults(t *testing.T) {
	content := `
metad: {}
storaged: {}
graphd: {}
`
	tmpFile, err := os.CreateTemp("", "cedar-test-defaults-*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatal(err)
	}
	tmpFile.Close()

	config, err := LoadConfig(tmpFile.Name())
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	if config.MetaD.Port != 9559 {
		t.Errorf("MetaD default port = %d, want 9559", config.MetaD.Port)
	}
	if config.MetaD.GrpcPort != 10559 {
		t.Errorf("MetaD default grpc_port = %d, want 10559", config.MetaD.GrpcPort)
	}
	if config.StorageD.Port != 9779 {
		t.Errorf("StorageD default port = %d, want 9779", config.StorageD.Port)
	}
	if config.GraphD.Port != 9669 {
		t.Errorf("GraphD default port = %d, want 9669", config.GraphD.Port)
	}
	if config.MetaD.BinaryPath != "build/cedar-metad" {
		t.Errorf("MetaD binary = %q, want build/cedar-metad", config.MetaD.BinaryPath)
	}
}

func TestLoadConfigFileNotFound(t *testing.T) {
	_, err := LoadConfig("/nonexistent/path/config.yaml")
	if err == nil {
		t.Error("expected error for nonexistent file")
	}
}

func TestLoadConfigInvalidYaml(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "cedar-test-invalid-*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())

	tmpFile.WriteString("{{invalid yaml}}")
	tmpFile.Close()

	_, err = LoadConfig(tmpFile.Name())
	if err == nil {
		t.Error("expected error for invalid YAML")
	}
}

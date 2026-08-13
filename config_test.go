package xtframework

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func validConfig() *Config {
	return &Config{
		MainNode: 1,
		Nodes: []NodeConfig{
			{ID: 1, ListenAddr: "127.0.0.1:7001", Services: []ServiceConfig{{Name: "center", ID: 1}}},
			{ID: 2, ListenAddr: "127.0.0.1:7002", Services: []ServiceConfig{{Name: "room", ID: 1}, {Name: "room", ID: 2}}},
		},
	}
}

func TestConfigValidate(t *testing.T) {
	if err := validConfig().Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{"missing main", func(c *Config) { c.MainNode = 3 }, "main_node"},
		{"duplicate node", func(c *Config) { c.Nodes[1].ID = 1 }, "duplicated"},
		{"empty address", func(c *Config) { c.Nodes[0].ListenAddr = "" }, "listen_addr"},
		{"duplicate address", func(c *Config) { c.Nodes[1].ListenAddr = c.Nodes[0].ListenAddr }, "also used"},
		{"empty service", func(c *Config) { c.Nodes[0].Services[0].Name = "" }, "name"},
		{"duplicate service", func(c *Config) { c.Nodes[1].Services[1].ID = 1 }, "duplicated"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := validConfig()
			test.mutate(cfg)
			err := cfg.Validate()
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Validate() error = %v, want containing %q", err, test.want)
			}
		})
	}
}

func TestLoadConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nodes.yaml")
	data := []byte("main_node: 1\ncodec: xtnet\nnodes:\n  - id: 1\n    listen_addr: 127.0.0.1:7001\n    services:\n      - name: center\n        id: 1\n")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MainNode != 1 || cfg.Codec != "xtnet" || len(cfg.Nodes) != 1 {
		t.Fatalf("unexpected config: %+v", cfg)
	}
}

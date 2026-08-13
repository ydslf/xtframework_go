package xtframework

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

// Config 描述一次部署中的所有节点。进程通过向 NewNode 传入节点 ID，
// 从配置中选择当前节点。
type Config struct {
	MainNode int          `yaml:"main_node"`
	Codec    string       `yaml:"codec,omitempty"`
	Nodes    []NodeConfig `yaml:"nodes"`
}

type NodeConfig struct {
	ID         int             `yaml:"id"`
	ListenAddr string          `yaml:"listen_addr"`
	Services   []ServiceConfig `yaml:"services,omitempty"`
}

type ServiceConfig struct {
	Name    string         `yaml:"name"`
	ID      int            `yaml:"id"`
	Options map[string]any `yaml:"options,omitempty"`
}

// LoadConfig 读取 YAML 配置文件并校验其结构。
// 由于服务工厂由应用程序在运行时提供，因此工厂可用性由 NewNode 校验。
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %q: %w", path, err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config %q: %w", path, err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validate config %q: %w", path, err)
	}
	return &cfg, nil
}

func (c *Config) Validate() error {
	if c == nil {
		return fmt.Errorf("config is nil")
	}
	if len(c.Nodes) == 0 {
		return fmt.Errorf("nodes must not be empty")
	}

	nodeIDs := make(map[int]struct{}, len(c.Nodes))
	addresses := make(map[string]int, len(c.Nodes))
	mainFound := false
	for i := range c.Nodes {
		n := &c.Nodes[i]
		if n.ID <= 0 {
			return fmt.Errorf("nodes[%d].id must be positive", i)
		}
		if _, exists := nodeIDs[n.ID]; exists {
			return fmt.Errorf("nodes[%d].id %d is duplicated", i, n.ID)
		}
		nodeIDs[n.ID] = struct{}{}
		if n.ID == c.MainNode {
			mainFound = true
		}

		n.ListenAddr = strings.TrimSpace(n.ListenAddr)
		if n.ListenAddr == "" {
			return fmt.Errorf("nodes[%d].listen_addr must not be empty", i)
		}
		if owner, exists := addresses[n.ListenAddr]; exists {
			return fmt.Errorf("nodes[%d].listen_addr %q is also used by node %d", i, n.ListenAddr, owner)
		}
		addresses[n.ListenAddr] = n.ID

		services := make(map[ServiceKey]struct{}, len(n.Services))
		for j := range n.Services {
			s := &n.Services[j]
			s.Name = strings.TrimSpace(s.Name)
			if s.Name == "" {
				return fmt.Errorf("nodes[%d].services[%d].name must not be empty", i, j)
			}
			if s.ID <= 0 {
				return fmt.Errorf("nodes[%d].services[%d].id must be positive", i, j)
			}
			key := ServiceKey{Name: s.Name, ID: s.ID}
			if _, exists := services[key]; exists {
				return fmt.Errorf("nodes[%d].services[%d] %s:%d is duplicated", i, j, s.Name, s.ID)
			}
			services[key] = struct{}{}
		}
	}

	if c.MainNode <= 0 || !mainFound {
		return fmt.Errorf("main_node %d does not refer to a configured node", c.MainNode)
	}
	return nil
}

func (c *Config) Node(id int) (NodeConfig, bool) {
	if c == nil {
		return NodeConfig{}, false
	}
	for _, node := range c.Nodes {
		if node.ID == id {
			return node, true
		}
	}
	return NodeConfig{}, false
}

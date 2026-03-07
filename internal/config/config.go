package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// NodeType defines the role of this proxy node in the pipeline.
//
//   Client ──► AccessNode ──► RelayNode(s) ──► EgressNode ──► Origin
type NodeType string

const (
	NodeTypeAccess NodeType = "access" // 接入端：面向客户端
	NodeTypeRelay  NodeType = "relay"  // 中间代理节点：多跳转发
	NodeTypeEgress NodeType = "egress" // 回源端：连接真实源站
)

// Protocol is the business protocol on the client-facing side.
type Protocol string

const (
	ProtocolTCP   Protocol = "tcp"   // L4 透明代理
	ProtocolHTTP  Protocol = "http"  // L7 HTTP 代理
	ProtocolHTTPS Protocol = "https" // L7 HTTPS 代理（TLS 卸载）
)

// Config is the root configuration structure.
type Config struct {
	Node     NodeConfig      `yaml:"node"`
	Services []ServiceConfig `yaml:"services"` // only for access nodes
	Tunnel   TunnelConfig    `yaml:"tunnel"`
	Log      LogConfig       `yaml:"log"`
}

// NodeConfig identifies this node.
type NodeConfig struct {
	Type NodeType `yaml:"type"`
	ID   string   `yaml:"id"`
}

// ServiceConfig maps a listening port to a business service.
// The access node uses this to determine where to route traffic.
type ServiceConfig struct {
	ID       string       `yaml:"id"`
	Port     int          `yaml:"port"`     // listening port that identifies this service
	Protocol Protocol     `yaml:"protocol"` // tcp | http | https
	Origin   OriginConfig `yaml:"origin"`   // final destination (used by egress)

	// NextHops lists WebSocket URLs of the next proxy layer.
	// Multiple entries enable load-balanced or failover forwarding.
	// Format: ws://host:port/tunnel  or  wss://host:port/tunnel
	NextHops []string `yaml:"next_hops"`

	// L7-specific settings (HTTP/HTTPS only)
	HTTP HTTPConfig `yaml:"http"`
}

// OriginConfig describes the real backend server.
type OriginConfig struct {
	Host           string        `yaml:"host"`
	Port           int           `yaml:"port"`
	TLS            bool          `yaml:"tls"`             // connect to origin with TLS
	DialTimeout    time.Duration `yaml:"dial_timeout"`    // default 10s
	ConnectTimeout time.Duration `yaml:"connect_timeout"` // default 30s
}

// HTTPConfig holds Layer-7 HTTP proxy settings.
type HTTPConfig struct {
	// Headers to add/override on forwarded requests.
	AddRequestHeaders map[string]string `yaml:"add_request_headers"`
	// Headers to strip from client requests before forwarding.
	RemoveRequestHeaders []string `yaml:"remove_request_headers"`
	// Whether to rewrite the Host header to the origin host.
	RewriteHost bool `yaml:"rewrite_host"`
}

// TunnelConfig configures the internal WebSocket tunnel endpoint.
// Relay and Egress nodes listen on this address for incoming tunnels.
type TunnelConfig struct {
	ListenAddr string        `yaml:"listen_addr"` // e.g. ":9000"
	Path       string        `yaml:"path"`        // WS path, default "/tunnel"
	TLS        TLSConfig     `yaml:"tls"`
	ReadTimeout  time.Duration `yaml:"read_timeout"`  // default 60s
	WriteTimeout time.Duration `yaml:"write_timeout"` // default 60s
	// Max size of a single WS frame in bytes (default 64KB)
	MaxFrameSize int64 `yaml:"max_frame_size"`
}

// TLSConfig holds TLS certificate paths for the tunnel listener.
type TLSConfig struct {
	Enabled  bool   `yaml:"enabled"`
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
}

// LogConfig controls logging behaviour.
type LogConfig struct {
	Level  string `yaml:"level"`  // debug | info | warn | error
	Format string `yaml:"format"` // text | json
}

// Load reads and validates a config file.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %q: %w", path, err)
	}

	cfg := defaultConfig()
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	return cfg, nil
}

func defaultConfig() *Config {
	return &Config{
		Tunnel: TunnelConfig{
			Path:         "/tunnel",
			ReadTimeout:  60 * time.Second,
			WriteTimeout: 60 * time.Second,
			MaxFrameSize: 64 * 1024,
		},
		Log: LogConfig{
			Level:  "info",
			Format: "text",
		},
	}
}

func (c *Config) validate() error {
	if c.Node.Type == "" {
		return fmt.Errorf("node.type is required")
	}
	if c.Node.ID == "" {
		return fmt.Errorf("node.id is required")
	}

	switch c.Node.Type {
	case NodeTypeAccess:
		if len(c.Services) == 0 {
			return fmt.Errorf("access node requires at least one service")
		}
		ports := make(map[int]bool)
		for i, svc := range c.Services {
			if svc.Port == 0 {
				return fmt.Errorf("service[%d].port is required", i)
			}
			if ports[svc.Port] {
				return fmt.Errorf("duplicate service port %d", svc.Port)
			}
			ports[svc.Port] = true
			if len(svc.NextHops) == 0 {
				return fmt.Errorf("service %q has no next_hops", svc.ID)
			}
		}

	case NodeTypeRelay:
		if c.Tunnel.ListenAddr == "" {
			return fmt.Errorf("relay node requires tunnel.listen_addr")
		}

	case NodeTypeEgress:
		if c.Tunnel.ListenAddr == "" {
			return fmt.Errorf("egress node requires tunnel.listen_addr")
		}

	default:
		return fmt.Errorf("unknown node type %q", c.Node.Type)
	}

	return nil
}

// ServiceByPort returns the ServiceConfig for the given listening port.
func (c *Config) ServiceByPort(port int) (*ServiceConfig, bool) {
	for i := range c.Services {
		if c.Services[i].Port == port {
			return &c.Services[i], true
		}
	}
	return nil, false
}

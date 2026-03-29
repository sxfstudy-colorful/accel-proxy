package config

import (
	"fmt"
	"net"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	maxAllowedFrameSize = 16 * 1024 * 1024
	defaultReadTimeout  = 60 * time.Second
	defaultWriteTimeout = 60 * time.Second
)

// ─────────────────────────────────────────────────────────────────────────────
// HopAddr
// ─────────────────────────────────────────────────────────────────────────────

type HopAddrType string

const (
	HopAddrTypeDomain HopAddrType = "domain"
	HopAddrTypeIP     HopAddrType = "ip"
)

type HopAddr struct {
	Type       HopAddrType `yaml:"type"`
	Addr       string      `yaml:"addr"`
	Port       int         `yaml:"port"`
	Host       string      `yaml:"host"`
	TLS        bool        `yaml:"tls"`
	TunnelPath string      `yaml:"tunnel_path"`
}

func (a HopAddr) DialAddr() string {
	return net.JoinHostPort(a.Addr, fmt.Sprintf("%d", a.Port))
}

func (a HopAddr) SNIHost() string {
	switch a.Type {
	case HopAddrTypeIP:
		return a.Host
	default:
		return a.Addr
	}
}

func (a HopAddr) WsURL() string {
	scheme := "ws"
	if a.TLS {
		scheme = "wss"
	}
	path := a.TunnelPath
	if path == "" {
		path = "/tunnel"
	}
	return fmt.Sprintf("%s://%s%s", scheme,
		net.JoinHostPort(a.SNIHost(), fmt.Sprintf("%d", a.Port)), path)
}

func (a HopAddr) Identity() string {
	path := a.TunnelPath
	if path == "" {
		path = "/tunnel"
	}
	return fmt.Sprintf("%s:%s:%d%s", a.Type, a.Addr, a.Port, path)
}

func (a HopAddr) Equal(b HopAddr) bool {
	return a.Type == b.Type &&
		a.Addr == b.Addr &&
		a.Port == b.Port &&
		a.Host == b.Host &&
		a.TLS == b.TLS &&
		a.TunnelPath == b.TunnelPath
}

func EqualHopAddrs(a, b []HopAddr) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !a[i].Equal(b[i]) {
			return false
		}
	}
	return true
}

type Protocol string

const (
	ProtocolTCP   Protocol = "tcp"
	ProtocolHTTP  Protocol = "http"
	ProtocolHTTPS Protocol = "https"
)

// ─────────────────────────────────────────────────────────────────────────────
// Root config
// ─────────────────────────────────────────────────────────────────────────────

type Config struct {
	Node   NodeConfig    `yaml:"node"`
	Access *AccessConfig `yaml:"access"`
	Relay  *RelayConfig  `yaml:"relay"`
	Egress *EgressConfig `yaml:"egress"`
	Log    LogConfig     `yaml:"log"`
}

type AccessConfig struct {
	MaxFrameSize int64           `yaml:"max_frame_size"`
	Services     []ServiceConfig `yaml:"services"`
}

type RelayConfig struct {
	Tunnel   TunnelConfig    `yaml:"tunnel"`
	Services []ServiceConfig `yaml:"services"`
}

type EgressConfig struct {
	Tunnel   TunnelConfig    `yaml:"tunnel"`
	Services []ServiceConfig `yaml:"services"`
}

type NodeConfig struct {
	ID  string `yaml:"id"`
	IDC string `yaml:"idc"`
}

// ─────────────────────────────────────────────────────────────────────────────
// ServiceConfig
// ─────────────────────────────────────────────────────────────────────────────

type ServiceConfig struct {
	ID       string   `yaml:"id"`
	Port     int      `yaml:"port"`
	Protocol Protocol `yaml:"protocol"`

	TargetIDC string       `yaml:"target_idc"`
	Groups    []RouteGroup `yaml:"groups"`
	Routes    []IDCRoute   `yaml:"routes"`
	Origin    OriginConfig `yaml:"origin"`
	HTTP      HTTPConfig   `yaml:"http"`
}

func (s *ServiceConfig) SortedHops() []HopAddr {
	return SortGroups(s.Groups)
}

// ─────────────────────────────────────────────────────────────────────────────
// Route hierarchy types
// ─────────────────────────────────────────────────────────────────────────────

type IDCRoute struct {
	IDC    string       `yaml:"idc"`
	Groups []RouteGroup `yaml:"groups"`
}

type RouteGroup struct {
	Name     string    `yaml:"name"`
	Priority int       `yaml:"priority"`
	Weight   int       `yaml:"weight"`
	Hops     []HopAddr `yaml:"hops"`
}

func (g *RouteGroup) GroupName() string {
	if g.Name == "" {
		return "default"
	}
	return g.Name
}

func (r *IDCRoute) SortedHops() []HopAddr {
	return SortGroups(r.Groups)
}

func HopsForIDC(routes []IDCRoute, idc string) []HopAddr {
	for i := range routes {
		if routes[i].IDC == idc {
			return routes[i].SortedHops()
		}
	}
	return nil
}

func SortGroups(groups []RouteGroup) []HopAddr {
	if len(groups) == 0 {
		return nil
	}
	sorted := make([]RouteGroup, len(groups))
	copy(sorted, groups)
	for i := 1; i < len(sorted); i++ {
		for j := i; j > 0 && sorted[j].Priority > sorted[j-1].Priority; j-- {
			sorted[j], sorted[j-1] = sorted[j-1], sorted[j]
		}
	}
	var out []HopAddr
	for i := range sorted {
		out = append(out, sorted[i].Hops...)
	}
	return out
}

// ─────────────────────────────────────────────────────────────────────────────
// OriginConfig / HTTPConfig
// ─────────────────────────────────────────────────────────────────────────────

type OriginConfig struct {
	Host           string        `yaml:"host"`
	Port           int           `yaml:"port"`
	TLS            bool          `yaml:"tls"`
	DialTimeout    time.Duration `yaml:"dial_timeout"`
	ConnectTimeout time.Duration `yaml:"connect_timeout"`
}

// HTTPConfig holds Layer-7 HTTP proxy settings (access nodes only).
type HTTPConfig struct {
	AddRequestHeaders    map[string]string `yaml:"add_request_headers"`
	RemoveRequestHeaders []string          `yaml:"remove_request_headers"`

	// RewriteHost enables rewriting the HTTP Host header.
	// When true, the Host header is set to RewriteHostValue.
	// If RewriteHostValue is empty, the rewrite is skipped (no-op).
	//
	// This replaces the old behavior that used Origin.Host:Port, which was
	// incorrect on access nodes where Origin is not configured.
	RewriteHost bool `yaml:"rewrite_host"`

	// RewriteHostValue is the value to set in the Host header when
	// RewriteHost is true. Example: "backend.internal:8080".
	// If empty and RewriteHost is true, a warning is logged and the
	// header is left unchanged.
	RewriteHostValue string `yaml:"rewrite_host_value"`
}

// ─────────────────────────────────────────────────────────────────────────────
// TunnelConfig
// ─────────────────────────────────────────────────────────────────────────────

type TunnelConfig struct {
	ListenAddr   string        `yaml:"listen_addr"`
	Path         string        `yaml:"path"`
	TLS          TLSConfig     `yaml:"tls"`
	ReadTimeout  time.Duration `yaml:"read_timeout"`
	WriteTimeout time.Duration `yaml:"write_timeout"`
	MaxFrameSize int64         `yaml:"max_frame_size"`
	PSK          string        `yaml:"psk"`
}

type TLSConfig struct {
	Enabled  bool   `yaml:"enabled"`
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
}

type LogConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

// ─────────────────────────────────────────────────────────────────────────────
// Load + validate
// ─────────────────────────────────────────────────────────────────────────────

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
		Log: LogConfig{Level: "info", Format: "text"},
	}
}

func (c *Config) validate() error {
	if c.Node.ID == "" {
		return fmt.Errorf("node.id is required")
	}
	if c.Access == nil && c.Relay == nil && c.Egress == nil {
		return fmt.Errorf("at least one role section (access/relay/egress) must be configured")
	}
	if c.Access != nil {
		if err := c.validateAccess(c.Access); err != nil {
			return fmt.Errorf("access: %w", err)
		}
	}
	if c.Relay != nil {
		if err := c.validateRelay(c.Relay); err != nil {
			return fmt.Errorf("relay: %w", err)
		}
	}
	if c.Egress != nil {
		if err := c.validateEgress(c.Egress); err != nil {
			return fmt.Errorf("egress: %w", err)
		}
	}
	return nil
}

func (c *Config) validateAccess(a *AccessConfig) error {
	if a.MaxFrameSize > maxAllowedFrameSize {
		return fmt.Errorf("max_frame_size %d exceeds maximum %d", a.MaxFrameSize, maxAllowedFrameSize)
	}
	ports := make(map[int]bool)
	for i, svc := range a.Services {
		if svc.Port == 0 {
			return fmt.Errorf("service[%d].port is required", i)
		}
		if ports[svc.Port] {
			return fmt.Errorf("duplicate service port %d", svc.Port)
		}
		ports[svc.Port] = true
		if svc.TargetIDC == "" {
			return fmt.Errorf("service %q: target_idc is required", svc.ID)
		}
		if len(svc.Groups) == 0 {
			return fmt.Errorf("service %q: at least one group required", svc.ID)
		}
		// Warn if rewrite_host is true but no target value is set.
		if svc.HTTP.RewriteHost && svc.HTTP.RewriteHostValue == "" {
			return fmt.Errorf("service %q: http.rewrite_host is true but http.rewrite_host_value is empty", svc.ID)
		}
		for j, g := range svc.Groups {
			if len(g.Hops) == 0 {
				return fmt.Errorf("service %q groups[%d] (%s): at least one hop required", svc.ID, j, g.GroupName())
			}
			for k, h := range g.Hops {
				if err := validateHop(h); err != nil {
					return fmt.Errorf("service %q groups[%d].hops[%d]: %w", svc.ID, j, k, err)
				}
			}
		}
	}
	return nil
}

func (c *Config) validateRelay(r *RelayConfig) error {
	if r.Tunnel.ListenAddr == "" {
		return fmt.Errorf("tunnel.listen_addr is required")
	}
	if r.Tunnel.MaxFrameSize > maxAllowedFrameSize {
		return fmt.Errorf("tunnel.max_frame_size %d exceeds maximum %d",
			r.Tunnel.MaxFrameSize, maxAllowedFrameSize)
	}
	if r.Tunnel.ReadTimeout == 0 {
		r.Tunnel.ReadTimeout = defaultReadTimeout
	}
	if r.Tunnel.WriteTimeout == 0 {
		r.Tunnel.WriteTimeout = defaultWriteTimeout
	}
	for i, svc := range r.Services {
		if len(svc.Routes) == 0 {
			return fmt.Errorf("service[%d] %q has no routes", i, svc.ID)
		}
		for j, rt := range svc.Routes {
			if rt.IDC == "" {
				return fmt.Errorf("service %q routes[%d]: idc is required", svc.ID, j)
			}
			if len(rt.Groups) == 0 {
				return fmt.Errorf("service %q routes[%d] (idc=%s): at least one group required", svc.ID, j, rt.IDC)
			}
			for k, g := range rt.Groups {
				if len(g.Hops) == 0 {
					return fmt.Errorf("service %q routes[%d].groups[%d] (%s): at least one hop required", svc.ID, j, k, g.GroupName())
				}
				for l, h := range g.Hops {
					if err := validateHop(h); err != nil {
						return fmt.Errorf("service %q routes[%d].groups[%d].hops[%d]: %w", svc.ID, j, k, l, err)
					}
				}
			}
		}
	}
	return nil
}

func (c *Config) validateEgress(e *EgressConfig) error {
	if e.Tunnel.ListenAddr == "" {
		return fmt.Errorf("tunnel.listen_addr is required")
	}
	if c.Node.IDC == "" {
		return fmt.Errorf("node.idc is required when egress is configured")
	}
	if e.Tunnel.MaxFrameSize > maxAllowedFrameSize {
		return fmt.Errorf("tunnel.max_frame_size %d exceeds maximum %d",
			e.Tunnel.MaxFrameSize, maxAllowedFrameSize)
	}
	if e.Tunnel.ReadTimeout == 0 {
		e.Tunnel.ReadTimeout = defaultReadTimeout
	}
	if e.Tunnel.WriteTimeout == 0 {
		e.Tunnel.WriteTimeout = defaultWriteTimeout
	}
	for i, svc := range e.Services {
		if svc.Origin.Host == "" {
			return fmt.Errorf("service[%d] %q: origin.host is required", i, svc.ID)
		}
		if svc.Origin.Port == 0 {
			return fmt.Errorf("service[%d] %q: origin.port is required", i, svc.ID)
		}
	}
	return nil
}

func validateHop(h HopAddr) error {
	if h.Type == "" {
		return fmt.Errorf("type is required (\"domain\" or \"ip\")")
	}
	if h.Addr == "" {
		return fmt.Errorf("addr is required")
	}
	if h.Port == 0 {
		return fmt.Errorf("port is required")
	}
	switch h.Type {
	case HopAddrTypeIP:
		if h.TLS && h.Host == "" {
			return fmt.Errorf("type=ip with tls=true requires host (TLS SNI cannot be derived from an IP)")
		}
	case HopAddrTypeDomain:
		if h.Host != "" {
			return fmt.Errorf("type=domain must not set host (addr %q is already the SNI)", h.Addr)
		}
	default:
		return fmt.Errorf("unknown type %q (want \"domain\" or \"ip\")", h.Type)
	}
	return nil
}

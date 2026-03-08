package config

import (
	"fmt"
	"net"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// ─────────────────────────────────────────────────────────────────────────────
// HopAddr — structured next-hop endpoint address
//
// Lives in config (not tunnel) to avoid a circular import:
//   tunnel/server.go → config   (needs TunnelConfig)
//   config           → HopAddr  (owns the type; no tunnel import needed)
//   tunnel/hop_pool  → config   (uses config.HopAddr)
//
// Rationale for structured fields over a raw URL string:
//
//  1. IP/Port independence from DNS — edge relays often have only an IP;
//     dialing works without any domain registration.
//
//  2. Explicit TLS SNI — when dialing by IP there is no hostname to derive
//     SNI from. Host fills that role without conflating "where to connect"
//     with "what name to present".
//
//  3. Clean hot-reload identity — identity is (IP, Port, TunnelPath).
//     An IP rotation (container restart, DHCP) keeps health and latency
//     history intact while updating only the address fields.
//
//  4. TCP-level probing — the prober dials IP:Port directly without
//     parsing a URL.
// ─────────────────────────────────────────────────────────────────────────────

// HopAddr is the address of one next-hop tunnel endpoint.
//
//   - IP:         dial target (IPv4/v6 literal). If empty, Host is used for dialing.
//   - Port:       TCP port (required).
//   - Host:       logical hostname for TLS SNI and HTTP Host header.
//   - TLS:        use wss:// when true.
//   - TunnelPath: WebSocket upgrade path; defaults to "/tunnel".
type HopAddr struct {
	IP         string `yaml:"ip"`
	Port       int    `yaml:"port"`
	Host       string `yaml:"host"`
	TLS        bool   `yaml:"tls"`
	TunnelPath string `yaml:"tunnel_path"`
}

// DialAddr returns the TCP address to connect to.
// Uses IP:Port when IP is set; falls back to Host:Port for DNS-only configs.
func (a HopAddr) DialAddr() string {
	ip := a.IP
	if ip == "" {
		ip = a.Host
	}
	return net.JoinHostPort(ip, fmt.Sprintf("%d", a.Port))
}

// WsURL builds the WebSocket URL from the structured fields.
// Constructed at dial time (not stored) so it always reflects current values.
func (a HopAddr) WsURL() string {
	scheme := "ws"
	if a.TLS {
		scheme = "wss"
	}
	path := a.TunnelPath
	if path == "" {
		path = "/tunnel"
	}
	host := a.Host
	if host == "" {
		host = a.IP
	}
	return fmt.Sprintf("%s://%s%s", scheme, net.JoinHostPort(host, fmt.Sprintf("%d", a.Port)), path)
}

// Identity returns a stable string key for this endpoint: IP+Port+TunnelPath.
// Host and TLS are excluded — they can change (cert rotation, SNI rename)
// without changing which physical machine we are talking to.
func (a HopAddr) Identity() string {
	ip := a.IP
	if ip == "" {
		ip = a.Host
	}
	path := a.TunnelPath
	if path == "" {
		path = "/tunnel"
	}
	return fmt.Sprintf("%s:%d%s", ip, a.Port, path)
}

// Equal reports whether two HopAddrs are fully identical (all fields).
func (a HopAddr) Equal(b HopAddr) bool {
	return a.IP == b.IP &&
		a.Port == b.Port &&
		a.Host == b.Host &&
		a.TLS == b.TLS &&
		a.TunnelPath == b.TunnelPath
}

// EqualHopAddrs compares two HopAddr slices by full field equality.
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

// NodeType defines the role of this proxy node in the pipeline.
//
//   Client ──► AccessNode ──► RelayNode(s) ──► EgressNode ──► Origin
type NodeType string

const (
	NodeTypeAccess NodeType = "access"
	NodeTypeRelay  NodeType = "relay"
	NodeTypeEgress NodeType = "egress"
)

// Protocol is the business protocol on the client-facing side.
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
	Node     NodeConfig     `yaml:"node"`
	Services []ServiceConfig `yaml:"services"`
	Tunnel   TunnelConfig   `yaml:"tunnel"`
	Log      LogConfig      `yaml:"log"`
}

// NodeConfig identifies this node.
type NodeConfig struct {
	Type NodeType `yaml:"type"`
	ID   string   `yaml:"id"`
	// IDC is the datacenter this node physically lives in.
	// Required for egress nodes (they serve the origin in this IDC).
	// Optional for access/relay (they only route toward a target IDC).
	IDC string `yaml:"idc"`
}

// ─────────────────────────────────────────────────────────────────────────────
// ServiceConfig
//
// The meaning of fields differs by node type:
//
//  Access node
//    - Port, Protocol  : client-facing listener
//    - TargetIDC       : which datacenter the origin lives in
//    - Routes          : idc → WebSocket URLs of next hop (relay/egress)
//    - HTTP            : L7 header manipulation
//
//  Relay node
//    - ID              : must match the ServiceID in HandshakeRequest
//    - Routes          : idc → WebSocket URLs of next hop (relay/egress)
//    (all other fields are ignored)
//
//  Egress node
//    - ID              : must match the ServiceID in HandshakeRequest
//    - Origin          : local origin address (in this IDC)
//    (all other fields are ignored)
// ─────────────────────────────────────────────────────────────────────────────

type ServiceConfig struct {
	ID       string   `yaml:"id"`
	Port     int      `yaml:"port"`     // access only: client-facing listen port
	Protocol Protocol `yaml:"protocol"` // access only: tcp | http | https

	// TargetIDC is the datacenter that holds the origin for this service.
	// Set on access nodes; carried unchanged through the tunnel as the routing key.
	TargetIDC string `yaml:"target_idc"`

	// Routes is the ordered list of IDC-level routing entries for this service.
	// Used by access and relay nodes to forward toward the correct datacenter.
	//
	// Hierarchy: Service → IDCRoute → RouteGroup → HopAddr
	//
	//   routes:
	//     - idc: idc-beijing
	//       groups:
	//         - name: primary       # optional; defaults to "default"
	//           hops:
	//             - ip: 10.0.1.5
	//               port: 9000
	//               host: relay-bj-01.internal
	//         - name: backup
	//           priority: -1        # lower priority = tried after primary groups
	//           hops:
	//             - ip: 10.0.1.6
	//               port: 9000
	//     - idc: idc-shanghai
	//       groups:
	//         - hops:
	//             - ip: 10.0.2.1
	//               port: 9000
	Routes []IDCRoute `yaml:"routes"`

	// Origin is the real backend address.
	// Only meaningful on egress nodes (local to their IDC).
	Origin OriginConfig `yaml:"origin"`

	// HTTP holds L7 header-manipulation settings (access nodes, http/https only).
	HTTP HTTPConfig `yaml:"http"`
}

// ─────────────────────────────────────────────────────────────────────────────
// Route hierarchy types
// ─────────────────────────────────────────────────────────────────────────────

// IDCRoute is the routing entry for one target datacenter.
// A ServiceConfig holds one IDCRoute per reachable IDC.
type IDCRoute struct {
	// IDC is the datacenter identifier this entry routes toward.
	IDC string `yaml:"idc"`

	// Groups is the ordered list of route groups within this IDC.
	// Groups are tried in descending Priority order (highest first).
	// Within a group, hops are distributed across the group's HopPool
	// according to the configured selector (default: round-robin).
	//
	// At least one group with at least one hop is required.
	Groups []RouteGroup `yaml:"groups"`
}

// RouteGroup is a named, weighted set of HopAddrs within one IDC.
//
// Use multiple groups per IDC to model primary/backup topology:
//
//	groups:
//	  - name: primary    # priority 0 (default) — tried first
//	    hops: [...]
//	  - name: backup
//	    priority: -1     # lower → tried only when primary group is exhausted
//	    hops: [...]
//
// Weight is reserved for future weighted-random selection between groups
// of equal priority.
type RouteGroup struct {
	// Name is an optional human-readable label used in logs and metrics.
	// Defaults to "default" if empty.
	Name string `yaml:"name"`

	// Priority controls the order in which groups are tried.
	// Higher value = tried first. Groups with equal priority are tried
	// in declaration order. Default: 0.
	Priority int `yaml:"priority"`

	// Weight is reserved for future weighted selection between same-priority
	// groups. Currently unused; all same-priority groups are treated equally.
	Weight int `yaml:"weight"`

	// Hops is the list of individual next-hop endpoints in this group.
	Hops []HopAddr `yaml:"hops"`
}

// GroupName returns the display name of the group (never empty).
func (g *RouteGroup) GroupName() string {
	if g.Name == "" {
		return "default"
	}
	return g.Name
}

// AllHops returns all HopAddrs across all groups in declaration order,
// without priority sorting. Used for simple flat lookups.
func (r *IDCRoute) AllHops() []HopAddr {
	var out []HopAddr
	for i := range r.Groups {
		out = append(out, r.Groups[i].Hops...)
	}
	return out
}

// NextHopsForIDC finds the IDCRoute for the given IDC and returns its hops
// ordered by group priority (highest first), then declaration order within
// each priority tier.
func (s *ServiceConfig) NextHopsForIDC(idc string) ([]HopAddr, bool) {
	route := s.IDCRouteFor(idc)
	if route == nil {
		return nil, false
	}
	hops := route.SortedHops()
	return hops, len(hops) > 0
}

// IDCRouteFor returns the IDCRoute for the given IDC, or nil if not found.
func (s *ServiceConfig) IDCRouteFor(idc string) *IDCRoute {
	for i := range s.Routes {
		if s.Routes[i].IDC == idc {
			return &s.Routes[i]
		}
	}
	return nil
}

// SortedHops returns all hops ordered by group priority descending.
// Groups with higher Priority values are placed first.
// Within the same priority tier, declaration order is preserved.
func (r *IDCRoute) SortedHops() []HopAddr {
	// Stable sort groups by descending priority.
	// N is tiny (typically 1–3 groups), so insertion sort is fine.
	sorted := make([]RouteGroup, len(r.Groups))
	copy(sorted, r.Groups)
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

// OriginConfig describes the real backend server (egress node only).
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
	RewriteHost          bool              `yaml:"rewrite_host"`
}

// ─────────────────────────────────────────────────────────────────────────────
// TunnelConfig — internal WebSocket tunnel listener (relay / egress nodes)
// ─────────────────────────────────────────────────────────────────────────────

type TunnelConfig struct {
	ListenAddr   string        `yaml:"listen_addr"`
	Path         string        `yaml:"path"`
	TLS          TLSConfig     `yaml:"tls"`
	ReadTimeout  time.Duration `yaml:"read_timeout"`
	WriteTimeout time.Duration `yaml:"write_timeout"`
	MaxFrameSize int64         `yaml:"max_frame_size"`
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
		Tunnel: TunnelConfig{
			Path:         "/tunnel",
			ReadTimeout:  60 * time.Second,
			WriteTimeout: 60 * time.Second,
			MaxFrameSize: 64 * 1024,
		},
		Log: LogConfig{Level: "info", Format: "text"},
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
		ports := make(map[int]bool)
		for i, svc := range c.Services {
			if svc.Port == 0 {
				return fmt.Errorf("service[%d].port is required", i)
			}
			if ports[svc.Port] {
				return fmt.Errorf("duplicate service port %d", svc.Port)
			}
			ports[svc.Port] = true
			if svc.TargetIDC == "" {
				return fmt.Errorf("service %q: target_idc is required on access node", svc.ID)
			}
			if _, ok := svc.NextHopsForIDC(svc.TargetIDC); !ok {
				return fmt.Errorf("service %q: routes has no entry for target_idc %q", svc.ID, svc.TargetIDC)
			}
		}

	case NodeTypeRelay:
		if c.Tunnel.ListenAddr == "" {
			return fmt.Errorf("relay node requires tunnel.listen_addr")
		}
		for i, svc := range c.Services {
			if len(svc.Routes) == 0 {
				return fmt.Errorf("relay service[%d] %q has no routes", i, svc.ID)
			}
			for j, r := range svc.Routes {
				if r.IDC == "" {
					return fmt.Errorf("relay service %q routes[%d]: idc is required", svc.ID, j)
				}
				if len(r.Groups) == 0 {
					return fmt.Errorf("relay service %q routes[%d] (idc=%s): at least one group required", svc.ID, j, r.IDC)
				}
				for k, g := range r.Groups {
					if len(g.Hops) == 0 {
						return fmt.Errorf("relay service %q routes[%d].groups[%d] (%s): at least one hop required", svc.ID, j, k, g.GroupName())
					}
				}
			}
		}

	case NodeTypeEgress:
		if c.Tunnel.ListenAddr == "" {
			return fmt.Errorf("egress node requires tunnel.listen_addr")
		}
		if c.Node.IDC == "" {
			return fmt.Errorf("egress node requires node.idc")
		}
		for i, svc := range c.Services {
			if svc.Origin.Host == "" {
				return fmt.Errorf("egress service[%d] %q: origin.host is required", i, svc.ID)
			}
			if svc.Origin.Port == 0 {
				return fmt.Errorf("egress service[%d] %q: origin.port is required", i, svc.ID)
			}
		}

	default:
		return fmt.Errorf("unknown node type %q", c.Node.Type)
	}

	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Lookup helpers
// ─────────────────────────────────────────────────────────────────────────────

// ServiceByPort returns the ServiceConfig for the given listening port (access node).
func (c *Config) ServiceByPort(port int) (*ServiceConfig, bool) {
	for i := range c.Services {
		if c.Services[i].Port == port {
			return &c.Services[i], true
		}
	}
	return nil, false
}

// ServiceByID returns the ServiceConfig for the given service ID.
func (c *Config) ServiceByID(id string) (*ServiceConfig, bool) {
	for i := range c.Services {
		if c.Services[i].ID == id {
			return &c.Services[i], true
		}
	}
	return nil, false
}

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

// HopAddrType identifies whether a HopAddr targets a domain name or a bare IP.
// The type determines how dialing and TLS SNI are handled:
//
//   - HopAddrTypeDomain: Addr is a resolvable domain name. The OS resolver
//     handles DNS. Addr doubles as the TLS SNI; Host is not needed.
//
//   - HopAddrTypeIP: Addr is an IPv4 or IPv6 literal. DNS is bypassed entirely.
//     When TLS is enabled, Host must be set to supply the SNI that the relay's
//     certificate is issued for — the IP itself carries no name information.
type HopAddrType string

const (
	// HopAddrTypeDomain connects via DNS resolution.
	// Addr must be a hostname; Host should be left empty.
	HopAddrTypeDomain HopAddrType = "domain"

	// HopAddrTypeIP connects directly to a bare IP address.
	// Addr must be an IPv4/IPv6 literal; Host is required for TLS.
	HopAddrTypeIP HopAddrType = "ip"
)

// HopAddr is the address of one next-hop tunnel endpoint.
//
// The Type field selects between two dialing modes:
//
//	# Domain-based relay (DNS resolves the address):
//	type: domain
//	addr: relay-bj-01.internal   # DNS name; also used as TLS SNI
//	port: 9000
//	tls:  true
//	# host: omit — domain is already the correct SNI
//
//	# IP-based edge relay (no DNS registration, TLS still required):
//	type: ip
//	addr: 10.0.1.6               # dialed directly, bypasses DNS
//	port: 9000
//	host: relay-bj-02.internal   # TLS SNI — must match the relay certificate
//	tls:  true
//
// Fields:
//
//	Type       — required. "domain" or "ip".
//	Addr       — required. Domain name (type=domain) or IP literal (type=ip).
//	Port       — required. TCP port.
//	Host       — TLS SNI / HTTP Host override. Required when type=ip and tls=true.
//	             Ignored when type=domain (Addr is already the correct name).
//	TLS        — use wss:// (WebSocket over TLS).
//	TunnelPath — WebSocket upgrade path; defaults to "/tunnel".
type HopAddr struct {
	Type       HopAddrType `yaml:"type"`
	Addr       string      `yaml:"addr"`
	Port       int         `yaml:"port"`
	Host       string      `yaml:"host"`        // SNI; required for ip+tls, ignored for domain
	TLS        bool        `yaml:"tls"`
	TunnelPath string      `yaml:"tunnel_path"`
}

// DialAddr returns the TCP address to connect to.
//
//   - domain: Addr:Port — the OS resolver handles DNS at dial time.
//   - ip:     Addr:Port — connects directly to the IP, no DNS query.
func (a HopAddr) DialAddr() string {
	return net.JoinHostPort(a.Addr, fmt.Sprintf("%d", a.Port))
}

// SNIHost returns the hostname to present in TLS ClientHello and HTTP Host header.
//
//   - domain: returns Addr — the domain name is its own SNI.
//   - ip:     returns Host — IP literals cannot carry certificate names; the
//             operator must explicitly supply the relay's hostname via Host.
//
// Returns an empty string only for a misconfigured ip-type hop with no Host.
// Callers should treat an empty SNIHost with TLS enabled as a configuration error.
func (a HopAddr) SNIHost() string {
	switch a.Type {
	case HopAddrTypeIP:
		return a.Host // explicit override required; may be empty (misconfiguration)
	default: // HopAddrTypeDomain and zero value
		return a.Addr
	}
}

// WsURL builds the WebSocket URL for the HTTP upgrade request.
// The URL hostname is always SNIHost() so TLS certificate validation uses the
// correct name. The actual TCP connection is made to DialAddr() via NetDial in
// dialWebSocketAddr, so domain and IP hops both connect to the right machine.
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

// Identity returns a stable string key used to match endpoints across hot-reloads.
// Key = Type+Addr+Port+TunnelPath. Host and TLS are excluded: the SNI name or
// TLS mode can change (cert rotation, hostname rename) without replacing the
// physical endpoint.
func (a HopAddr) Identity() string {
	path := a.TunnelPath
	if path == "" {
		path = "/tunnel"
	}
	return fmt.Sprintf("%s:%s:%d%s", a.Type, a.Addr, a.Port, path)
}

// Equal reports whether two HopAddrs are fully identical (all fields).
func (a HopAddr) Equal(b HopAddr) bool {
	return a.Type == b.Type &&
		a.Addr == b.Addr &&
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
// Fields by node type:
//
//  Access node
//    - Port, Protocol  : client-facing listener
//    - TargetIDC       : which datacenter the origin lives in
//    - Groups          : route groups toward TargetIDC (no idc wrapper; the
//                        destination is already given by target_idc)
//    - HTTP            : L7 header manipulation
//
//  Relay node
//    - ID              : must match the ServiceID in HandshakeRequest
//    - Routes          : per-IDC routing table (multiple IDC destinations)
//
//  Egress node
//    - ID              : must match the ServiceID in HandshakeRequest
//    - Origin          : local origin address (in this IDC)
// ─────────────────────────────────────────────────────────────────────────────

type ServiceConfig struct {
	ID       string   `yaml:"id"`
	Port     int      `yaml:"port"`     // access only
	Protocol Protocol `yaml:"protocol"` // access only: tcp | http | https

	// TargetIDC is the datacenter that holds the origin for this service.
	// Set on access nodes; forwarded unchanged as the tunnel routing key.
	TargetIDC string `yaml:"target_idc"` // access only

	// Groups is the list of route groups toward TargetIDC.
	// Access nodes only. Because a service has exactly one TargetIDC, no idc
	// wrapper is needed — Groups directly lists the next-hop options.
	//
	//   target_idc: idc-beijing
	//   groups:
	//     - name: primary
	//       hops:
	//         - host: relay-bj-01.internal
	//           port: 9000
	//         - ip: 10.0.1.6
	//           port: 9000
	//           host: relay-bj-02.internal
	//     - name: backup
	//       priority: -1   # tried only when primary is fully unavailable
	//       hops:
	//         - ip: 10.0.1.9
	//           port: 9000
	Groups []RouteGroup `yaml:"groups"` // access only

	// Routes is the per-IDC routing table.
	// Relay nodes only. Each entry names a target IDC so the relay can forward
	// to multiple downstream datacenters from a single service definition.
	//
	//   routes:
	//     - idc: idc-beijing
	//       groups:
	//         - hops:
	//             - ip: 10.1.0.1
	//               port: 9100
	//     - idc: idc-hongkong
	//       groups:
	//         - hops:
	//             - ip: 10.2.0.1
	//               port: 9100
	Routes []IDCRoute `yaml:"routes"` // relay only

	// Origin is the real backend address (egress only).
	Origin OriginConfig `yaml:"origin"`

	// HTTP holds L7 header-manipulation settings (access, http/https only).
	HTTP HTTPConfig `yaml:"http"`
}

// SortedHops returns the priority-sorted hops from the access node's Groups.
// Higher-priority groups come first; within a tier, declaration order is kept.
func (s *ServiceConfig) SortedHops() []HopAddr {
	return SortGroups(s.Groups)
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

// SortedHops returns this IDCRoute's hops ordered by group priority descending.
// Groups with equal priority are kept in declaration order (stable sort).
func (r *IDCRoute) SortedHops() []HopAddr {
	return SortGroups(r.Groups)
}

// HopsForIDC finds the IDCRoute for the given IDC and returns its
// priority-sorted hops. Returns nil if no entry matches.
// Used by relay nodes to resolve a TargetIDC to a hop list.
func HopsForIDC(routes []IDCRoute, idc string) []HopAddr {
	for i := range routes {
		if routes[i].IDC == idc {
			return routes[i].SortedHops()
		}
	}
	return nil
}

// SortGroups sorts groups by descending priority and flattens their hops.
// Insertion sort — N is tiny (1–3 groups in practice).
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
			if len(svc.Groups) == 0 {
				return fmt.Errorf("service %q: at least one group required", svc.ID)
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
					for l, h := range g.Hops {
						if err := validateHop(h); err != nil {
							return fmt.Errorf("relay service %q routes[%d].groups[%d].hops[%d]: %w", svc.ID, j, k, l, err)
						}
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

// validateHop checks that a HopAddr is correctly formed for its declared type.
//
// Rules:
//   - type and addr are always required.
//   - type=ip + tls=true requires host (TLS certificate name cannot be derived
//     from an IP literal; the relay certificate must match a hostname).
//   - type=domain must not set host (Addr already is the SNI; setting Host
//     would cause a mismatch between the dial target and the certificate check).
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
			return fmt.Errorf("type=domain must not set host (addr %q is already the SNI; setting host would cause a name mismatch)", h.Addr)
		}
	default:
		return fmt.Errorf("unknown type %q (want \"domain\" or \"ip\")", h.Type)
	}
	return nil
}

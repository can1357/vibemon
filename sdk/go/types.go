package vmon

import (
	"encoding/json"
	"fmt"
)

// Health is the response from the daemon health endpoint.
type Health struct {
	// OK reports whether the daemon considers itself healthy.
	OK bool `json:"ok"`
}

// ServerInfo describes the daemon build and host capabilities.
type ServerInfo struct {
	// Version is the daemon version.
	Version string `json:"version"`
	// Platform is the daemon operating system.
	Platform string `json:"platform"`
	// Arch is the daemon architecture.
	Arch string `json:"arch"`
	// Backend is the active virtualization backend.
	Backend string `json:"backend"`
	// Capabilities reports feature support by capability name.
	Capabilities map[string]bool `json:"capabilities"`
}

// Sandbox is a typed view of a daemon-owned sandbox.
type Sandbox struct {
	// ID is the stable sandbox identifier.
	ID string `json:"id"`
	// Name is the human-facing sandbox name.
	Name string `json:"name"`
	// Status is the current lifecycle state.
	Status string `json:"status"`
	// PID is the monitor process identifier when one is available.
	PID *int32 `json:"pid,omitempty"`
	// Source is the image, template, fork, or restore source.
	Source *string `json:"source,omitempty"`
	// CreatedAt is the Unix creation timestamp.
	CreatedAt float64 `json:"created_at"`
	// LastActive is the Unix timestamp of the last activity.
	LastActive float64 `json:"last_active"`
	// ExpiresAt is the Unix idle-timeout deadline when armed.
	ExpiresAt *float64 `json:"expires_at"`
	// TerminatedAt is the Unix termination timestamp when terminated.
	TerminatedAt *float64 `json:"terminated_at"`
	// ErrorMessage is the terminal daemon error, when present.
	ErrorMessage *string `json:"error"`
	// Tags contains the sandbox's string tags.
	Tags map[string]string `json:"tags"`
	// ReturnCode is the foreground process exit code when known.
	ReturnCode *int64 `json:"returncode"`
	// Details retains non-canonical response fields as raw JSON.
	Details map[string]json.RawMessage `json:"-"`

	client *Client
}

// UnmarshalJSON decodes canonical fields while preserving open sandbox detail fields.
func (sandbox *Sandbox) UnmarshalJSON(data []byte) error {
	type sandboxAlias Sandbox
	var decoded sandboxAlias
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	var details map[string]json.RawMessage
	if err := json.Unmarshal(data, &details); err != nil {
		return err
	}
	for _, key := range []string{
		"id", "name", "status", "pid", "source", "created_at", "last_active",
		"expires_at", "terminated_at", "error", "tags", "returncode",
	} {
		delete(details, key)
	}
	*sandbox = Sandbox(decoded)
	sandbox.Details = details
	return nil
}

// SandboxCreateRequest is the stable request body for creating a sandbox.
type SandboxCreateRequest struct {
	// Image is an image reference.
	Image string `json:"image,omitempty"`
	// Template is a cached template reference.
	Template string `json:"template,omitempty"`
	// Dockerfile is inline Dockerfile content.
	Dockerfile string `json:"dockerfile,omitempty"`
	// Context is the image build context.
	Context string `json:"context,omitempty"`
	// Name requests a sandbox name.
	Name string `json:"name,omitempty"`
	// CPUs is the virtual CPU count.
	CPUs uint32 `json:"cpus,omitempty"`
	// MemoryMiB is guest memory in MiB.
	MemoryMiB uint32 `json:"memory,omitempty"`
	// DiskMiB is guest disk size in MiB.
	DiskMiB uint32 `json:"disk_mb,omitempty"`
	// Timeout is the request timeout in seconds.
	Timeout *float64 `json:"timeout,omitempty"`
	// TimeoutSeconds is the sandbox idle timeout in seconds.
	TimeoutSeconds *uint64 `json:"timeout_secs,omitempty"`
	// Workdir is the default guest working directory.
	Workdir string `json:"workdir,omitempty"`
	// Env contains non-secret environment variables.
	Env map[string]string `json:"env,omitempty"`
	// Secrets contains request-scoped secret bundles.
	Secrets []Secret `json:"secrets,omitempty"`
	// Volumes maps guest mountpoints to named volume mounts.
	Volumes map[string]VolumeMount `json:"volumes,omitempty"`
	// Tags contains sandbox metadata tags.
	Tags map[string]string `json:"tags,omitempty"`
	// FilesystemDirectory selects a host filesystem source where supported.
	FilesystemDirectory string `json:"fs_dir,omitempty"`
	// BlockNetwork disables guest network access.
	BlockNetwork bool `json:"block_network,omitempty"`
	// Ports lists guest TCP ports to expose.
	Ports []uint16 `json:"ports,omitempty"`
	// EgressAllow lists allowed destination CIDRs.
	EgressAllow []string `json:"egress_allow,omitempty"`
	// EgressAllowDomains lists allowed destination domains.
	EgressAllowDomains []string `json:"egress_allow_domains,omitempty"`
	// InboundCIDRAllowlist lists source CIDRs allowed to reach exposed ports.
	InboundCIDRAllowlist []string `json:"inbound_cidr_allowlist,omitempty"`
	// ReadinessProbe is the daemon-defined readiness probe value.
	ReadinessProbe any `json:"readiness_probe,omitempty"`
	// PoolSize requests a warm pool size.
	PoolSize uint32 `json:"pool_size,omitempty"`
	// HA selects the high-availability policy.
	HA string `json:"ha,omitempty"`
	// Arch selects the guest architecture.
	Arch string `json:"arch,omitempty"`
	// IdempotencyKey makes repeated creates resolve to the same request.
	IdempotencyKey string `json:"idempotency_key,omitempty"`
	// Command overrides the foreground entrypoint.
	Command []string `json:"command,omitempty"`
}

// SandboxListOptions filters sandbox list requests.
type SandboxListOptions struct {
	// Tags requires each key/value tag pair to match.
	Tags map[string]string
}

// SandboxPoll is one non-blocking sandbox lifecycle observation.
type SandboxPoll struct {
	// Sandbox is the latest view, or nil when the resource no longer exists.
	Sandbox *Sandbox
	// Exists reports whether the sandbox still exists.
	Exists bool
	// Done reports whether the sandbox reached a terminal state or no longer exists.
	Done bool
	// ExitCode is the foreground exit code when the daemon reported one.
	ExitCode *int64
}

// ExtendResult is the daemon response after extending a sandbox idle deadline.
type ExtendResult struct {
	// DeadlineUnix is the updated absolute Unix deadline.
	DeadlineUnix int64 `json:"deadline_unix"`
}

// MigrateResult is the mesh-defined object returned after migrating a sandbox.
type MigrateResult map[string]any

// ExecRequest describes a command to run inside a sandbox.
type ExecRequest struct {
	// Command is the non-empty command argument vector sent as cmd.
	Command []string `json:"cmd"`
	// Workdir is the guest working directory.
	Workdir string `json:"workdir,omitempty"`
	// Env contains per-process environment variables.
	Env map[string]string `json:"env,omitempty"`
	// Timeout is the server-side timeout in seconds.
	Timeout *float64 `json:"timeout,omitempty"`
	// TTY requests a pseudoterminal.
	TTY bool `json:"tty,omitempty"`
}

// ExecResult is the decoded captured-exec response.
type ExecResult struct {
	// ExitCode is the process exit code.
	ExitCode int64
	// Stdout is the captured standard output.
	Stdout []byte
	// Stderr is the captured standard error.
	Stderr []byte
}

// ExecExit describes the terminal event of a streaming exec or shell.
type ExecExit struct {
	// Code is the process exit code.
	Code int64
	// Signal is the terminating signal when one was reported.
	Signal *int
}

// StreamName identifies an exec or attach output stream.
type StreamName string

const (
	// StreamStdout identifies standard output.
	StreamStdout StreamName = "stdout"
	// StreamStderr identifies standard error.
	StreamStderr StreamName = "stderr"
	// StreamConsole identifies attached console output.
	StreamConsole StreamName = "console"
)

// ExecEvent is one output or exit event from a streaming exec.
type ExecEvent struct {
	// Stream identifies Data's source; it is empty for an exit event.
	Stream StreamName
	// Data contains decoded stream bytes.
	Data []byte
	// Exit is non-nil for the terminal event.
	Exit *ExecExit
}

// StreamEvent is one decoded output event from an attach stream.
type StreamEvent struct {
	// Stream identifies Data's source.
	Stream StreamName
	// Data contains decoded stream bytes.
	Data []byte
}

// FileInfo describes one guest filesystem entry.
type FileInfo struct {
	// OK is set by stat responses.
	OK bool `json:"ok,omitempty"`
	// Name is populated by directory-list responses.
	Name string `json:"name,omitempty"`
	// Type is the daemon's file type string.
	Type string `json:"type"`
	// Size is the file size in bytes.
	Size uint64 `json:"size"`
	// Mode is the guest file mode.
	Mode uint32 `json:"mode"`
	// ModTime is the guest Unix modification time.
	ModTime int64 `json:"mtime"`
}

// NetworkPolicy is the update body for a sandbox network policy.
type NetworkPolicy struct {
	// BlockNetwork changes whether all guest network access is blocked.
	BlockNetwork *bool `json:"block_network,omitempty"`
	// CIDRAllow non-nil replaces the allowed destination CIDRs, including with an empty list.
	CIDRAllow *[]string `json:"cidr_allow,omitempty"`
	// DomainAllow non-nil replaces the allowed destination domains, including with an empty list.
	DomainAllow *[]string `json:"domain_allow,omitempty"`
}

// NetworkState describes the effective persisted sandbox network settings.
type NetworkState struct {
	// BlockNetwork reports whether network access is blocked.
	BlockNetwork *bool `json:"block_network"`
	// CIDRAllow contains response-native allowed CIDRs when supplied by a peer.
	CIDRAllow []string `json:"cidr_allow,omitempty"`
	// DomainAllow contains response-native allowed domains when supplied by a peer.
	DomainAllow []string `json:"domain_allow,omitempty"`
	// EgressAllow contains persisted allowed destination CIDRs.
	EgressAllow []string `json:"egress_allow"`
	// EgressAllowDomains contains persisted allowed destination domains.
	EgressAllowDomains []string `json:"egress_allow_domains"`
	// InboundCIDRAllowlist contains persisted inbound source CIDRs.
	InboundCIDRAllowlist []string `json:"inbound_cidr_allowlist"`
}

// TunnelTarget identifies the daemon-side target for one exposed guest port.
type TunnelTarget struct {
	// Host is the target host address.
	Host string `json:"host"`
	// Port is the target TCP port.
	Port uint16 `json:"port"`
}

// TunnelSet contains exposed ports and the short-lived proxy connection token.
type TunnelSet struct {
	// Tunnels maps guest port numbers to daemon-side targets.
	Tunnels map[uint16]TunnelTarget `json:"tunnels"`
	// ConnectToken authenticates HTTP and WebSocket port proxy requests.
	ConnectToken string `json:"connect_token"`
}

// SnapshotRequest is the request body for a full sandbox snapshot.
type SnapshotRequest struct {
	// Name requests a snapshot name.
	Name string `json:"name,omitempty"`
	// Stop stops the sandbox while taking the snapshot.
	Stop bool `json:"stop,omitempty"`
}

// SnapshotResult is the response from a full sandbox snapshot.
type SnapshotResult struct {
	// Snapshot is the created snapshot name.
	Snapshot string `json:"snapshot"`
}

// FilesystemSnapshotRequest is the request body for a filesystem-only snapshot.
type FilesystemSnapshotRequest struct {
	// Name requests an image name.
	Name string `json:"name,omitempty"`
}

// FilesystemSnapshotResult is the response from a filesystem-only snapshot.
type FilesystemSnapshotResult struct {
	// Image is the created filesystem image name.
	Image string `json:"image"`
}

// RestoreRequest describes a snapshot restore and optional create-style overrides.
type RestoreRequest struct {
	// Name requests a sandbox name.
	Name string
	// Agent controls whether restore waits for the guest agent.
	Agent *bool
	// Overrides contains additional create-style fields.
	Overrides map[string]any
}

// MarshalJSON merges restore fields and overrides into the daemon request shape.
func (request RestoreRequest) MarshalJSON() ([]byte, error) {
	for _, key := range []string{"name", "agent"} {
		if _, exists := request.Overrides[key]; exists {
			return nil, fmt.Errorf("vmon: override %q conflicts with a typed request field", key)
		}
	}
	known := make(map[string]any, 2)
	if request.Name != "" {
		known["name"] = request.Name
	}
	if request.Agent != nil {
		known["agent"] = *request.Agent
	}
	return marshalWithExtras(known, request.Overrides)
}

// ForkRequest describes an ordered batch of clones from a snapshot.
type ForkRequest struct {
	// Count is the positive number of clones to create.
	Count uint32
	// Overrides contains additional create-style fields.
	Overrides map[string]any
}

// MarshalJSON merges the fork count and create-style overrides.
func (request ForkRequest) MarshalJSON() ([]byte, error) {
	return marshalWithExtras(map[string]any{"count": request.Count}, request.Overrides)
}

// ForkResult contains the sandbox views created by a snapshot fork.
type ForkResult struct {
	// Clones is the ordered list of created sandbox views.
	Clones []*Sandbox `json:"clones"`
}

// PoolRequest describes a warm-pool size and template overrides.
type PoolRequest struct {
	// Size is the desired warm-pool size.
	Size uint32
	// Template contains image-pipeline template arguments.
	Template map[string]any
}

// MarshalJSON merges the pool size and template arguments.
func (request PoolRequest) MarshalJSON() ([]byte, error) {
	return marshalWithExtras(map[string]any{"size": request.Size}, request.Template)
}

// PoolStats is one warm-pool statistics snapshot.
type PoolStats struct {
	// Ready is the number of ready sandboxes.
	Ready uint64 `json:"ready"`
	// Hits is the number of successful pool acquisitions.
	Hits uint64 `json:"hits"`
	// Misses is the number of pool misses.
	Misses uint64 `json:"misses"`
	// Size is the configured pool size.
	Size uint64 `json:"size"`
}

// ShellRequest describes a WebSocket shell session.
type ShellRequest struct {
	// Reference names an existing sandbox, template, or image.
	Reference string `json:"ref,omitempty"`
	// Image explicitly selects an image for an ephemeral shell.
	Image string `json:"image,omitempty"`
	// Command is the shell command argument vector.
	Command []string `json:"cmd,omitempty"`
	// CPUs is the ephemeral shell virtual CPU count.
	CPUs uint32 `json:"cpus,omitempty"`
	// MemoryMiB is the ephemeral shell memory in MiB.
	MemoryMiB uint32 `json:"mem,omitempty"`
	// DiskMiB is the ephemeral shell disk size in MiB.
	DiskMiB uint32 `json:"disk_mb,omitempty"`
	// Timeout is the ephemeral shell timeout in seconds.
	Timeout *float64 `json:"timeout,omitempty"`
}

// Event is one dynamic lifecycle event from the server-sent event stream.
type Event map[string]any

func marshalWithExtras(known map[string]any, extras map[string]any) ([]byte, error) {
	for key, value := range extras {
		if _, reserved := known[key]; reserved {
			return nil, fmt.Errorf("vmon: override %q conflicts with a typed request field", key)
		}
		known[key] = value
	}
	return json.Marshal(known)
}

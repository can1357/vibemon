package vmon

import (
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
)

var volumeNamePattern = regexp.MustCompile(`^[a-z0-9_][a-z0-9_.-]{0,63}$`)

// Secret is an in-memory bundle of environment values sent only in create requests.
type Secret struct {
	name   string
	values map[string]string
}

// NewSecret validates and copies a named set of secret environment values.
func NewSecret(name string, values map[string]string) (Secret, error) {
	if err := validateEnvironmentName(name); err != nil {
		return Secret{}, fmt.Errorf("vmon: invalid secret name: %w", err)
	}
	copied := make(map[string]string, len(values))
	for key, value := range values {
		if err := validateEnvironmentName(key); err != nil {
			return Secret{}, fmt.Errorf("vmon: invalid secret environment name %q: %w", key, err)
		}
		if strings.ContainsRune(value, '\x00') {
			return Secret{}, fmt.Errorf("vmon: secret environment value %q contains NUL", key)
		}
		copied[key] = value
	}
	return Secret{name: name, values: copied}, nil
}

// SecretFromEnvironment captures the requested variables that exist in the process environment.
func SecretFromEnvironment(name string, variables ...string) (Secret, error) {
	values := make(map[string]string, len(variables))
	for _, variable := range variables {
		if err := validateEnvironmentName(variable); err != nil {
			return Secret{}, fmt.Errorf("vmon: invalid secret environment name %q: %w", variable, err)
		}
		if value, exists := os.LookupEnv(variable); exists {
			values[variable] = value
		}
	}
	return NewSecret(name, values)
}

// Name returns the secret bundle name.
func (secret Secret) Name() string {
	return secret.name
}

// Names returns sorted environment variable names without exposing their values.
func (secret Secret) Names() []string {
	names := make([]string, 0, len(secret.values))
	for name := range secret.values {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// MarshalJSON encodes the request-only v1 secret wire shape.
func (secret Secret) MarshalJSON() ([]byte, error) {
	if err := validateEnvironmentName(secret.name); err != nil {
		return nil, fmt.Errorf("vmon: invalid secret: %w", err)
	}
	values := make(map[string]string, len(secret.values))
	for key, value := range secret.values {
		if err := validateEnvironmentName(key); err != nil {
			return nil, fmt.Errorf("vmon: invalid secret environment name %q: %w", key, err)
		}
		if strings.ContainsRune(value, '\x00') {
			return nil, fmt.Errorf("vmon: secret environment value %q contains NUL", key)
		}
		values[key] = value
	}
	return json.Marshal(struct {
		Name   string            `json:"name"`
		Values map[string]string `json:"values"`
	}{Name: secret.name, Values: values})
}

// String returns a redacted description of the secret.
func (secret Secret) String() string {
	return fmt.Sprintf("Secret(name=%q, variables=%v)", secret.name, secret.Names())
}

// GoString returns a redacted Go-syntax description of the secret.
func (secret Secret) GoString() string {
	return secret.String()
}

func validateEnvironmentName(name string) error {
	if name == "" {
		return fmt.Errorf("must not be empty")
	}
	if strings.ContainsAny(name, "=\x00") {
		return fmt.Errorf("must contain neither '=' nor NUL")
	}
	return nil
}

// Volume is a validated server-owned persistent volume name.
type Volume struct {
	name string
}

// NewVolume validates a persistent volume name.
func NewVolume(name string) (Volume, error) {
	if !volumeNamePattern.MatchString(name) {
		return Volume{}, fmt.Errorf("vmon: invalid volume name %q: must match %s", name, volumeNamePattern)
	}
	return Volume{name: name}, nil
}

// Name returns the validated server-side volume name.
func (volume Volume) Name() string {
	return volume.name
}

// Mount creates a sandbox volume-mount value.
func (volume Volume) Mount(readOnly bool) VolumeMount {
	return VolumeMount{volume: volume, readOnly: readOnly}
}

// String returns the volume name.
func (volume Volume) String() string {
	return volume.name
}

// VolumeMount is a validated named-volume mount used in SandboxCreateRequest.
type VolumeMount struct {
	volume   Volume
	readOnly bool
}

// Volume returns the named volume backing the mount.
func (mount VolumeMount) Volume() Volume {
	return mount.volume
}

// ReadOnly reports whether the guest mount is read-only.
func (mount VolumeMount) ReadOnly() bool {
	return mount.readOnly
}

// MarshalJSON encodes the v1 string or read-only object volume shape.
func (mount VolumeMount) MarshalJSON() ([]byte, error) {
	if !volumeNamePattern.MatchString(mount.volume.name) {
		return nil, fmt.Errorf("vmon: invalid volume mount name %q", mount.volume.name)
	}
	if !mount.readOnly {
		return json.Marshal(mount.volume.name)
	}
	return json.Marshal(struct {
		Name     string `json:"name"`
		ReadOnly bool   `json:"read_only"`
	}{Name: mount.volume.name, ReadOnly: true})
}

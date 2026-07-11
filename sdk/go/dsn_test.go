package vmon

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestParseDSNGrammar(t *testing.T) {
	t.Setenv("VMON_API_TOKEN", "")
	tests := []struct {
		name      string
		dsn       string
		endpoints []string
		token     string
		discover  bool
		timeout   time.Duration
	}{
		{name: "multi host", dsn: "vmon://a,b:9000/api?token=t&discover=off&timeout=1.5", endpoints: []string{"http://a:8000/api", "http://b:9000/api"}, token: "t", timeout: 1500 * time.Millisecond},
		{name: "secure IPv6", dsn: "vmons://[::1]", endpoints: []string{"https://[::1]:8000"}, discover: true, timeout: time.Minute},
		{name: "HTTP verbatim", dsn: "http://example.test/prefix/", endpoints: []string{"http://example.test/prefix"}, discover: true, timeout: time.Minute},
		{name: "unix", dsn: "vmon+unix:///tmp/vmond.sock?discover=off", endpoints: []string{"vmon+unix:///tmp/vmond.sock"}, timeout: time.Minute},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config, err := ParseDSN(test.dsn)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(config.Endpoints, test.endpoints) || config.Token != test.token || config.Discover != test.discover || config.Timeout != test.timeout {
				t.Fatalf("ParseDSN() = %#v", config)
			}
		})
	}
}

func TestParseDSNContextAndEnvironmentPrecedence(t *testing.T) {
	home := t.TempDir()
	if err := os.MkdirAll(filepath.Join(home, "credentials"), 0700); err != nil {
		t.Fatal(err)
	}
	contexts := `{"contexts":{"prod":{"name":"prod","endpoints":["https://a.test/base"],"region":"x","updated":1}}}`
	if err := os.WriteFile(filepath.Join(home, "contexts.json"), []byte(contexts), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(home, "credentials", "prod.token"), []byte("saved\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("VMON_HOME", home)
	t.Setenv("VMON_CONTEXT", "prod")
	t.Setenv("VMON_API_TOKEN", "environment")

	config, err := ParseDSN("")
	if err != nil {
		t.Fatal(err)
	}
	if config.Token != "environment" || !reflect.DeepEqual(config.Endpoints, []string{"https://a.test/base"}) {
		t.Fatalf("environment context = %#v", config)
	}
	config, err = ParseDSN("vmon+context://prod?token=dsn")
	if err != nil {
		t.Fatal(err)
	}
	if config.Token != "dsn" {
		t.Fatalf("DSN token = %q", config.Token)
	}
	t.Setenv("VMON_API_TOKEN", "")
	config, err = ParseDSN("vmon+context://prod")
	if err != nil {
		t.Fatal(err)
	}
	if config.Token != "saved" {
		t.Fatalf("saved token = %q", config.Token)
	}
}

func TestParseDSNErrors(t *testing.T) {
	t.Setenv("VMON_API_TOKEN", "")
	values := []string{
		"vmon://a?unknown=x",
		"vmon://a?token=x&token=y",
		"vmon://a?discover=yes",
		"vmon://a?timeout=0",
		"vmon://user@a",
		"http://a,http://b",
		"vmon+unix://host/tmp/x.sock",
		"vmon+unix://relative.sock",
		"ftp://a",
		"vmon://a#fragment",
	}
	for _, value := range values {
		t.Run(strings.ReplaceAll(value, "/", "_"), func(t *testing.T) {
			if _, err := ParseDSN(value); err == nil {
				t.Fatalf("ParseDSN(%q) unexpectedly succeeded", value)
			}
		})
	}
}

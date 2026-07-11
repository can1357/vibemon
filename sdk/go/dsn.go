package vmon

import (
	"errors"
	"fmt"
	"math"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const defaultVmonPort = "8000"

// DSNConfig is the endpoint roster and connection policy resolved from a DSN.
type DSNConfig struct {
	Endpoints []string
	Token     string
	Discover  bool
	Timeout   time.Duration
}

// ParseDSN parses a vmon connection string without making a network request.
func ParseDSN(dsn string) (DSNConfig, error) {
	if dsn == "" {
		dsn = os.Getenv("VMON_DSN")
		if dsn == "" {
			if contextName := os.Getenv("VMON_CONTEXT"); contextName != "" {
				dsn = "vmon+context://" + contextName
			} else {
				dsn = "vmon+unix://" + filepath.ToSlash(filepath.Join(vmonStateDir(), "vmond.sock"))
			}
		}
	}
	dsn = strings.TrimSpace(dsn)
	if dsn == "" {
		return DSNConfig{}, errors.New("DSN is empty")
	}
	parsed, err := url.Parse(dsn)
	if err != nil {
		return DSNConfig{}, fmt.Errorf("parse DSN: %w", err)
	}
	if parsed.Fragment != "" {
		return DSNConfig{}, errors.New("DSN fragments are not supported")
	}
	token, discover, timeout, err := parseDSNOptions(parsed.RawQuery)
	if err != nil {
		return DSNConfig{}, err
	}

	var endpoints []string
	scheme := strings.ToLower(parsed.Scheme)
	switch scheme {
	case "vmon", "vmons":
		if parsed.User != nil || strings.Contains(parsed.Host, "@") {
			return DSNConfig{}, errors.New("userinfo is not allowed in a DSN")
		}
		if parsed.Host == "" {
			return DSNConfig{}, errors.New("vmon DSN requires at least one host")
		}
		hosts, splitErr := splitDSNHosts(parsed.Host)
		if splitErr != nil {
			return DSNConfig{}, splitErr
		}
		endpointScheme := "http"
		if scheme == "vmons" {
			endpointScheme = "https"
		}
		for _, host := range hosts {
			endpoint, normalizeErr := normalizeHTTPEndpoint(endpointScheme+"://"+host+parsed.EscapedPath(), defaultVmonPort)
			if normalizeErr != nil {
				return DSNConfig{}, normalizeErr
			}
			endpoints = append(endpoints, endpoint)
		}
	case "http", "https":
		if strings.Contains(parsed.Host, ",") {
			return DSNConfig{}, errors.New("http and https DSNs accept one endpoint")
		}
		endpoint, normalizeErr := normalizeHTTPEndpoint(dsnWithoutQuery(parsed), "")
		if normalizeErr != nil {
			return DSNConfig{}, normalizeErr
		}
		endpoints = []string{endpoint}
	case "vmon+unix":
		if parsed.Host != "" {
			return DSNConfig{}, errors.New("vmon+unix DSN must not contain a host")
		}
		path, unescapeErr := url.PathUnescape(parsed.EscapedPath())
		if unescapeErr != nil {
			return DSNConfig{}, fmt.Errorf("invalid vmon+unix socket path: %w", unescapeErr)
		}
		if !filepath.IsAbs(path) {
			return DSNConfig{}, errors.New("vmon+unix DSN requires an absolute socket path")
		}
		endpoints = []string{"vmon+unix://" + filepath.ToSlash(path)}
	case "vmon+context":
		if parsed.User != nil || strings.Contains(parsed.Host, "@") {
			return DSNConfig{}, errors.New("userinfo is not allowed in a DSN")
		}
		if parsed.Host == "" || (parsed.Path != "" && parsed.Path != "/") {
			return DSNConfig{}, errors.New("vmon+context DSN requires a context name")
		}
		name, unescapeErr := url.PathUnescape(parsed.Host)
		if unescapeErr != nil {
			return DSNConfig{}, fmt.Errorf("invalid context name: %w", unescapeErr)
		}
		if !validContextName(name) {
			return DSNConfig{}, fmt.Errorf("invalid context name %q", name)
		}
		context, loadErr := loadStoredContext(name)
		if loadErr != nil {
			return DSNConfig{}, loadErr
		}
		if len(context.Endpoints) == 0 {
			return DSNConfig{}, fmt.Errorf("context %q has no endpoints", name)
		}
		for _, raw := range context.Endpoints {
			if raw == "" {
				continue
			}
			endpoint, normalizeErr := normalizeHTTPEndpoint(raw, "")
			if normalizeErr != nil {
				return DSNConfig{}, normalizeErr
			}
			endpoints = append(endpoints, endpoint)
		}
		if len(endpoints) == 0 {
			return DSNConfig{}, fmt.Errorf("context %q has no endpoints", name)
		}
		if token == "" {
			token = os.Getenv("VMON_API_TOKEN")
		}
		if token == "" {
			token = loadContextToken(name)
		}
	default:
		return DSNConfig{}, fmt.Errorf("unsupported DSN scheme %q", parsed.Scheme)
	}
	if scheme != "vmon+context" && token == "" {
		token = os.Getenv("VMON_API_TOKEN")
	}
	return DSNConfig{Endpoints: endpoints, Token: token, Discover: discover, Timeout: timeout}, nil
}

func validContextName(name string) bool {
	if name == "" {
		return false
	}
	for index := range len(name) {
		character := name[index]
		if (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') || (character >= '0' && character <= '9') {
			continue
		}
		if index != 0 && (character == '.' || character == '_' || character == '-') {
			continue
		}
		return false
	}
	return true
}

func parseDSNOptions(rawQuery string) (string, bool, time.Duration, error) {
	query, err := url.ParseQuery(rawQuery)
	if err != nil {
		return "", false, 0, fmt.Errorf("invalid DSN query: %w", err)
	}
	for key, values := range query {
		if key != "token" && key != "discover" && key != "timeout" {
			return "", false, 0, fmt.Errorf("invalid DSN parameter %s", key)
		}
		if len(values) != 1 {
			return "", false, 0, fmt.Errorf("duplicate DSN parameter %s", key)
		}
	}
	discoverValue := query.Get("discover")
	if discoverValue == "" {
		discoverValue = "on"
	}
	if discoverValue != "on" && discoverValue != "off" {
		return "", false, 0, errors.New("DSN discover must be 'on' or 'off'")
	}
	timeoutValue := query.Get("timeout")
	if timeoutValue == "" {
		timeoutValue = "60"
	}
	seconds, err := strconv.ParseFloat(timeoutValue, 64)
	if err != nil || math.IsNaN(seconds) || math.IsInf(seconds, 0) || seconds <= 0 || seconds > float64(math.MaxInt64)/float64(time.Second) {
		return "", false, 0, errors.New("DSN timeout must be a positive number")
	}
	return query.Get("token"), discoverValue == "on", time.Duration(seconds * float64(time.Second)), nil
}

func splitDSNHosts(authority string) ([]string, error) {
	var hosts []string
	start, depth := 0, 0
	for index, character := range authority {
		switch character {
		case '[':
			depth++
		case ']':
			depth--
			if depth < 0 {
				return nil, errors.New("invalid DSN host list")
			}
		case ',':
			if depth == 0 {
				hosts = append(hosts, authority[start:index])
				start = index + 1
			}
		}
	}
	if depth != 0 {
		return nil, errors.New("invalid DSN host list")
	}
	hosts = append(hosts, authority[start:])
	for _, host := range hosts {
		if host == "" {
			return nil, errors.New("invalid DSN host list")
		}
	}
	return hosts, nil
}

func normalizeHTTPEndpoint(value, defaultPort string) (string, error) {
	parsed, err := url.Parse(value)
	if err != nil {
		return "", fmt.Errorf("invalid endpoint %q: %w", value, err)
	}
	parsed.Scheme = strings.ToLower(parsed.Scheme)
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", fmt.Errorf("unsupported endpoint scheme %q", parsed.Scheme)
	}
	if parsed.User != nil || strings.Contains(parsed.Host, "@") {
		return "", errors.New("userinfo is not allowed in a DSN")
	}
	if parsed.Hostname() == "" {
		return "", fmt.Errorf("invalid endpoint %q", value)
	}
	port := parsed.Port()
	if port == "" {
		port = defaultPort
	}
	host := parsed.Hostname()
	if port != "" {
		host = net.JoinHostPort(host, port)
	} else if strings.Contains(host, ":") {
		host = "[" + host + "]"
	}
	parsed.Host = host
	parsed.Path = strings.TrimRight(parsed.Path, "/")
	parsed.RawPath = strings.TrimRight(parsed.RawPath, "/")
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed.String(), nil
}

func dsnWithoutQuery(parsed *url.URL) string {
	copy := *parsed
	copy.RawQuery = ""
	copy.Fragment = ""
	return copy.String()
}

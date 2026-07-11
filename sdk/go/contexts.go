package vmon

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type storedContext struct {
	Name      string   `json:"name"`
	Endpoints []string `json:"endpoints"`
	Region    string   `json:"region"`
	Updated   float64  `json:"updated"`
}

type contextStoreFile struct {
	Current  string                   `json:"current"`
	Contexts map[string]storedContext `json:"contexts"`
}

func vmonStateDir() string {
	if home := os.Getenv("VMON_HOME"); home != "" {
		return home
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ".vmon"
	}
	return filepath.Join(home, ".vmon")
}

func loadStoredContext(name string) (storedContext, error) {
	path := filepath.Join(vmonStateDir(), "contexts.json")
	encoded, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return storedContext{}, errors.New("context " + strconv.Quote(name) + " was not found")
		}
		return storedContext{}, err
	}
	var store contextStoreFile
	if err := json.Unmarshal(encoded, &store); err != nil {
		return storedContext{}, err
	}
	context, ok := store.Contexts[name]
	if !ok {
		return storedContext{}, errors.New("context " + strconv.Quote(name) + " was not found")
	}
	if context.Name == "" {
		context.Name = name
	}
	return context, nil
}

func loadContextToken(name string) string {
	encoded, err := os.ReadFile(filepath.Join(vmonStateDir(), "credentials", name+".token"))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(encoded))
}

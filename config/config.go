package config

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"log"
	"os"
	"path/filepath"
)

type KodbConfig struct {
	DatabaseConfig DatabaseConfig `yaml:"databaseConfig"`
	SchemaConfig   SchemaConfig   `yaml:"schemaConfig"`
}

type DatabaseConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Instance string `yaml:"instance"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
}

type SchemaConfig struct {
	Dir    string       `yaml:"dir"`
	GameDb GenDbConfig  `yaml:"gameDb"`
	Users  []UserConfig `yaml:"users"`
}

type GenDbConfig struct {
	Name    string   `yaml:"name"`
	Schemas []string `yaml:"schemas"`
	Logins  []Login  `yaml:"logins"`
}

type Login struct {
	Name string `yaml:"name"`
	Pass string `yaml:"pass"`
}
type UserConfig struct {
	Name   string `yaml:"name"`
	Schema string `yaml:"schema"`
}

const (
	DefaultConfigFileName = "kodb-util-config.yaml"
)

var (
	// Override config file passed via CLI argument
	// this path is relative to the working directory; not this source file
	ConfigPath = ""

	// private - will act as a singleton
	config *KodbConfig
)

// GetConfig returns a singleton instance of KodbConfig containing the application's configuration.
// can throw panic if config cannot be loaded
func GetConfig() *KodbConfig {
	if config == nil {
		loadConfig()
	}

	return config
}

func loadConfig() {
	// Failing to load config is one of the few areas where we'll do a panic instead of error handling
	if ConfigPath == "" {
		ConfigPath = DefaultConfigFileName
	}
	absPath, pErr := filepath.Abs(ConfigPath)
	if pErr != nil {
		log.Panic(fmt.Errorf("failed to parse path for config: %v", pErr))
	}

	yamlFile, err := os.ReadFile(absPath)
	if err != nil {
		log.Panic(fmt.Errorf("failed to read config.yaml: %v", err))
	}

	err = yaml.Unmarshal(yamlFile, &config)
	if err != nil {
		log.Panic(fmt.Errorf("failed to parse config.yaml: %v", err))
	}

}

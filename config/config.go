package config

import (
	"encoding/json"
	"log"
	"os"
)

type Config struct {
	General struct {
		ClientId       string `json:"clientId"`
		GroupBlacklist string `json:"groupBlacklist"`
		Logconfig      string `json:"logconfig"`
		Pidfile        string `json:"pidfile"`
	} `json:"general"`

	Influxdb struct {
		Db       string `json:"db"`
		Enable   bool   `json:"enable"`
		Hosts    string `json:"hosts"`
		Pwd      string `json:"pwd"`
		Username string `json:"username"`
	} `json:"influxdb"`

	Kafka map[string]*struct {
		Brokers       string `json:"brokers"`
		Zookeepers    string `json:"zookeepers"`
		OffsetTopic   string `json:"offsetTopic"`
		ClientProfile string `json:"ClientProfile"`
		OffsetsTopic  string `gcfg:"offsetsTopic"`
	} `json:"kafka"`

	Zookeeper struct {
		Hosts     string `json:"hosts"`
		Lock_path string `json:"lock-path"`
		Timeout   int    `json:"timeout"`
	} `json:"zookeeper"`

	ClientProfile map[string]*Profile `json:"ClientProfile"`
}

type Profile struct {
	ClientId        string `json:"clientId"`
	TLS             bool   `json:"tls"`
	TLSNoVerify     bool   `json:"tlsNoverify"`
	TLSCertFilePath string `json:"tlsCertfilepath"`
	TLSKeyFilePath  string `json:"tlsKeyfilepath"`
	TLSCAFilePath   string `json:"tlsCafilepath"`
}

func ReadConfig(cfgFile string) *Config {
	var cfg Config
	f, err := os.OpenFile(cfgFile, os.O_RDONLY, 0660)
	errAndExit(err)
	err = json.NewDecoder(f).Decode(&cfg)
	errAndExit(err)

	cfg.Init()
	return &cfg
}

func (cfg *Config) Init() {
	if cfg.ClientProfile == nil {
		cfg.ClientProfile = make(map[string]*Profile)
	}
	if _, ok := cfg.ClientProfile["default"]; !ok {
		cfg.ClientProfile["default"] = &Profile{
			ClientId: cfg.General.ClientId,
			TLS:      false,
		}
	}

	for _, k := range cfg.Kafka {
		if k.OffsetTopic == "" {
			k.OffsetTopic = "__consumer_offsets"
		}
		if k.ClientProfile == "" {
			k.ClientProfile = "default"
		}
	}
}

func errAndExit(err error) {
	if err != nil {
		log.Fatalf("Failed to parse json data: %s", err)
		os.Exit(1)
	}
}

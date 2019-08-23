package config

import (
	"encoding/json"
	"log"
	"os"
)

type Config struct {
	General struct {
		Logconfig string `json:"logconfig"`
		Pidfile   string `json:"pidfile"`

		TopicFilter string `json:"topicFilter"`
		GroupFilter string `json:"groupFilter"`
	} `json:"general"`

	Kafka map[string]*struct {
		Brokers       string  `json:"brokers"`
		ClientProfile Profile `json:"ClientProfile"`

		Sasl struct {
			Username string
			Password string
		}
	} `json:"kafka"`

	Outputs []M `json:"outputs"`
}

type M map[string]interface{}

type Profile struct {
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

}

func errAndExit(err error) {
	if err != nil {
		log.Fatalf("Failed to parse json data: %s", err)
		os.Exit(1)
	}
}

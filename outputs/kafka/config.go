package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"

	"github.com/Shopify/sarama"
	"github.com/sundy-li/burrowx/config"
)

func BuildKafkaConfig(tlsProfile config.Profile, sasl config.Sasl, version string) (*sarama.Config, error) {
	k := sarama.NewConfig()

	if tlsProfile.TLS {
		k.Net.TLS.Enable = true
		caCert, err := ioutil.ReadFile(tlsProfile.TLSCAFilePath)
		if err != nil {
			return nil, err
		}
		cert, err := tls.LoadX509KeyPair(tlsProfile.TLSCertFilePath, tlsProfile.TLSKeyFilePath)
		if err != nil {
			return nil, err
		}

		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)
		k.Net.TLS.Config = &tls.Config{
			Certificates: []tls.Certificate{cert},
			RootCAs:      caCertPool,
		}
		k.Net.TLS.Config.BuildNameToCertificate()
		k.Net.TLS.Config.InsecureSkipVerify = tlsProfile.TLSNoVerify
	}

	if sasl.Username != "" {
		k.Net.SASL.Enable = true
		k.Net.SASL.User = sasl.Username
		k.Net.SASL.Password = sasl.Password
	}

	if err := k.Validate(); err != nil {
		return nil, err
	}
	kversion, ok := kafkaVersions[version]
	if !ok {
		return nil, fmt.Errorf("Unknown/unsupported kafka version: %v", version)
	}
	k.Version = kversion
	return k, nil
}

var (
	compressionModes = map[string]sarama.CompressionCodec{
		"none":   sarama.CompressionNone,
		"no":     sarama.CompressionNone,
		"off":    sarama.CompressionNone,
		"gzip":   sarama.CompressionGZIP,
		"lz4":    sarama.CompressionLZ4,
		"snappy": sarama.CompressionSnappy,
	}

	kafkaVersions = map[string]sarama.KafkaVersion{
		"": sarama.V0_8_2_0,

		"0.8.2.0": sarama.V0_8_2_0,
		"0.8.2.1": sarama.V0_8_2_1,
		"0.8.2.2": sarama.V0_8_2_2,
		"0.8.2":   sarama.V0_8_2_2,
		"0.8":     sarama.V0_8_2_2,

		"0.9.0.0": sarama.V0_9_0_0,
		"0.9.0.1": sarama.V0_9_0_1,
		"0.9.0":   sarama.V0_9_0_1,
		"0.9":     sarama.V0_9_0_1,

		"0.10.0.0": sarama.V0_10_0_0,
		"0.10.0.1": sarama.V0_10_0_1,
		"0.10.0":   sarama.V0_10_0_1,
		"0.10.1.0": sarama.V0_10_1_0,
		"0.10.1":   sarama.V0_10_1_0,
		"0.10.2.0": sarama.V0_10_2_0,
		"0.10.2.1": v0_10_2_1,
		"0.10.2":   v0_10_2_1,
		"0.10":     v0_10_2_1,

		"0.11.0.0": v0_11_0_0,
		"0.11.0.1": v0_11_0_0,
		"0.11.0.2": v0_11_0_0,
		"0.11.0":   v0_11_0_0,
		"0.11":     v0_11_0_0,

		"1.0.0": sarama.V1_0_0_0,
	}
)

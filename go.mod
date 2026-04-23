module github.com/DataDog/kafka-kit/v4

go 1.25.0

require (
	github.com/Masterminds/semver v1.5.0
	github.com/confluentinc/confluent-kafka-go v1.9.2
	github.com/go-zookeeper/zk v1.0.4
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.29.0
	github.com/jamiealquiza/envy v1.1.0
	github.com/spf13/cobra v1.10.2
	github.com/stretchr/testify v1.8.4
	github.com/zorkian/go-datadog-api v2.30.0+incompatible
	google.golang.org/genproto/googleapis/api v0.0.0-20260414002931-afd174a4e478
	google.golang.org/grpc v1.80.0
	google.golang.org/grpc/cmd/protoc-gen-go-grpc v1.6.1
	google.golang.org/protobuf v1.36.11
	gopkg.in/DataDog/dd-trace-go.v1 v1.66.0
)

require (
	github.com/DataDog/datadog-go/v5 v5.6.0 // indirect
	github.com/DataDog/gostackparse v0.7.0 // indirect
	github.com/Microsoft/go-winio v0.6.2 // indirect
	github.com/cenkalti/backoff v2.2.1+incompatible // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/google/pprof v0.0.0-20250403155104-27863c87afa6 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/richardartoul/molecule v1.0.1-0.20240531184615-7ca0df43c0b3 // indirect
	github.com/rogpeppe/go-internal v1.14.1 // indirect
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	github.com/spf13/pflag v1.0.9 // indirect
	github.com/stretchr/objx v0.5.2 // indirect
	go.yaml.in/yaml/v3 v3.0.4 // indirect
	golang.org/x/net v0.49.0 // indirect
	golang.org/x/sys v0.40.0 // indirect
	golang.org/x/text v0.36.0 // indirect
	golang.org/x/time v0.11.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260414002931-afd174a4e478 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/spf13/viper v1.10.0 => github.com/spf13/viper v1.10.1

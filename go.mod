module github.com/DataDog/kafka-kit/v4

go 1.17

require (
	github.com/Masterminds/semver v1.5.0
	github.com/confluentinc/confluent-kafka-go v1.9.1
	github.com/go-zookeeper/zk v1.0.2
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.11.0
	github.com/jamiealquiza/envy v1.1.0
	github.com/spf13/cobra v1.5.0
	github.com/stretchr/testify v1.7.1
	github.com/zorkian/go-datadog-api v2.30.0+incompatible
	google.golang.org/genproto v0.0.0-20220720214146-176da50484ac
	google.golang.org/grpc v1.48.0
	google.golang.org/grpc/cmd/protoc-gen-go-grpc v1.1.0
	google.golang.org/protobuf v1.28.0
	gopkg.in/DataDog/dd-trace-go.v1 v1.40.1
)

require (
	github.com/DataDog/datadog-go v4.8.3+incompatible // indirect
	github.com/DataDog/datadog-go/v5 v5.1.1 // indirect
	github.com/DataDog/gostackparse v0.5.0 // indirect
	github.com/Microsoft/go-winio v0.5.2 // indirect
	github.com/cenkalti/backoff v2.2.1+incompatible // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/golang/glog v1.0.0 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/google/pprof v0.0.0-20220608213341-c488b8fa1db3 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/inconshreveable/mousetrap v1.0.0 // indirect
	github.com/opentracing/opentracing-go v1.2.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/stretchr/objx v0.2.0 // indirect
	golang.org/x/net v0.0.0-20220708220712-1185a9018129 // indirect
	golang.org/x/sys v0.0.0-20220721230656-c6bc011c0c49 // indirect
	golang.org/x/text v0.3.7 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	sigs.k8s.io/yaml v1.3.0 // indirect
)

replace github.com/spf13/viper v1.10.0 => github.com/spf13/viper v1.10.1

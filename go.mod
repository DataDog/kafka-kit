module github.com/DataDog/kafka-kit/v3

go 1.17

require (
	github.com/Masterminds/semver v1.5.0
	github.com/confluentinc/confluent-kafka-go v1.8.2
	github.com/go-zookeeper/zk v1.0.2
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.7.2
	github.com/jamiealquiza/envy v1.1.0
	github.com/spf13/cobra v1.3.0
	github.com/stretchr/testify v1.7.0
	github.com/zorkian/go-datadog-api v2.30.0+incompatible
	google.golang.org/genproto v0.0.0-20211208223120-3a66f561d7aa
	google.golang.org/grpc v1.43.0
	google.golang.org/grpc/cmd/protoc-gen-go-grpc v1.1.0
	google.golang.org/protobuf v1.27.1
	gopkg.in/DataDog/dd-trace-go.v1 v1.34.0
)

require (
	github.com/DataDog/datadog-go v4.4.0+incompatible // indirect
	github.com/DataDog/gostackparse v0.5.0 // indirect
	github.com/Microsoft/go-winio v0.5.1 // indirect
	github.com/cenkalti/backoff v2.2.1+incompatible // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/golang/glog v1.0.0 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/google/pprof v0.0.0-20210720184732-4bb14d4b1be1 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/inconshreveable/mousetrap v1.0.0 // indirect
	github.com/opentracing/opentracing-go v1.2.0 // indirect
	github.com/philhofer/fwd v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/stretchr/objx v0.1.1 // indirect
	golang.org/x/net v0.0.0-20210813160813-60bc85c4be6d // indirect
	golang.org/x/sys v0.0.0-20211210111614-af8b64212486 // indirect
	golang.org/x/text v0.3.7 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b // indirect
	sigs.k8s.io/yaml v1.3.0 // indirect
)

replace github.com/spf13/viper v1.10.0 => github.com/spf13/viper v1.10.1

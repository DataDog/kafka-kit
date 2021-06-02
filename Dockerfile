from ubuntu:21.04

# Install pre-reqs.
ARG DEBIAN_FRONTEND=noninteractive
RUN apt update >/dev/null
RUN apt install -y build-essential unzip curl git pkg-config software-properties-common apt-transport-https ca-certificates >/dev/null

WORKDIR /root

# Install Go.
RUN curl -sOL https://golang.org/dl/go1.16.4.linux-amd64.tar.gz
RUN tar -C /usr/local -xzf go1.16.4.linux-amd64.tar.gz
ENV PATH=$PATH:/usr/local/go/bin:/go/bin
ENV GOPATH=/go

# Install librdkafka
RUN curl -sL https://packages.confluent.io/deb/5.3/archive.key | apt-key add - 2>/dev/null
RUN add-apt-repository "deb [arch=amd64] https://packages.confluent.io/deb/5.3 stable main"
RUN apt-get update && apt-get install -y librdkafka1 librdkafka-dev >/dev/null

# Init repo.
WORKDIR /go/src/github.com/DataDog/kafka-kit
COPY go.mod go.mod
RUN go mod download

# Install protoc, grpc-gateway.
RUN go get github.com/golang/protobuf/protoc-gen-go
RUN go install github.com/grpc-ecosystem/grpc-gateway/protoc-gen-grpc-gateway
RUN curl -sOL https://github.com/protocolbuffers/protobuf/releases/download/v3.17.1/protoc-3.17.1-linux-x86_64.zip
RUN unzip protoc-3.17.1-linux-x86_64.zip -d protoc
RUN mv protoc/bin/* /usr/local/bin/
RUN mv protoc/include/* /usr/local/include/
RUN rm -rf protoc*

# Copy src.
COPY cmd cmd
COPY kafkaadmin kafkaadmin
COPY kafkametrics kafkametrics
COPY kafkazk kafkazk
COPY registry registry

# Build.
RUN protoc -I registry -I $GOPATH/pkg/mod/$(awk '/grpc-gateway/ {printf "%s@%s", $1, $2}' go.mod)/third_party/googleapis protos/registry.proto --go_out=plugins=grpc:$GOPATH/src --grpc-gateway_out=logtostderr=true:$GOPATH/src
RUN go install ./cmd/registry

//go:build tools
// +build tools

// This file exists solely to ensure build dependencies without explicit imports
// in code aren't removed via go tidy.

package tools

import (
	_ "github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-grpc-gateway"
	_ "google.golang.org/grpc/cmd/protoc-gen-go-grpc"
	_ "google.golang.org/protobuf/cmd/protoc-gen-go"
)

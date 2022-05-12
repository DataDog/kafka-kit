EXECUTABLES = docker go
K := $(foreach exec,$(EXECUTABLES),\
        $(if $(shell which $(exec)),some string,$(error "$(exec) not found in PATH")))

# Run a Docker compose environment.
run-compose: compose-build
	docker-compose up -d

# Tear down the Docker compose environment.
stop-compose:
	docker-compose down

# Ensure any local images used by compose are up to date.
compose-build:
	docker-compose build

# Build the Kafka-Kit image.
build-image:
	docker buildx build --load --platform linux/amd64 -t kafka-kit -f Dockerfile .

# Run unit tests.
test:
	go test -v ./...

# Run all tests.
integration-test: compose-build
	docker-compose run --rm --name integration-test registry go test -timeout 30s --tags=integration ./...

# Generate proto code outputs.
generate-code: build-image
	docker create --name kafka-kit kafka-kit >/dev/null; \
	docker cp kafka-kit:/go/src/github.com/DataDog/kafka-kit/registry/registry/. ${CURDIR}/registry/registry; \
	docker rm kafka-kit >/dev/null

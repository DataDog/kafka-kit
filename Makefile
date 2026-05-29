EXECUTABLES = docker go
K := $(foreach exec,$(EXECUTABLES),\
        $(if $(shell which $(exec)),some string,$(error "$(exec) not found in PATH")))

# Run a Docker compose environment.
run-compose: compose-build
	docker compose up -d --wait

# Tear down the Docker compose environment.
stop-compose:
	docker compose down

kill-compose:
	docker compose kill

# Ensure any local images used by compose are up to date.
compose-build:
	docker compose build

# Build the Kafka-Kit image.
build-image:
	docker buildx build --load --platform linux/amd64 -t kafka-kit --target base -f Dockerfile .

# Run unit tests.
test:
	go test -v ./...

# Run all tests.
integration-test: stop-compose build-image run-compose
	docker run --platform linux/amd64 --rm --network kafka-kit_default --name integration-test kafka-kit go test -timeout 30s --tags integration ./...

# Generate proto code outputs.
generate-code: build-image
	docker create --platform linux/amd64 --name kafka-kit kafka-kit >/dev/null; \
	docker cp kafka-kit:/go/src/github.com/DataDog/kafka-kit/proto/registrypb/. ${CURDIR}/proto/registrypb; \
	docker rm kafka-kit >/dev/null


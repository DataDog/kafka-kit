EXECUTABLES = docker
K := $(foreach exec,$(EXECUTABLES),\
        $(if $(shell which $(exec)),some string,$(error "$(exec) not found in PATH")))

build-container:
	docker compose build

run-compose:
	docker compose up -d

integration-test: build-container
	docker-compose run --rm --name registry_test registry go test -timeout 1m --tags=integration ./...

clean:
	docker compose down

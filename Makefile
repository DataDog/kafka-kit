EXECUTABLES = docker
K := $(foreach exec,$(EXECUTABLES),\
        $(if $(shell which $(exec)),some string,$(error "$(exec) not found in PATH")))

build-container:
	docker compose build

run-compose:
	docker compose up -d --scale kafka=3

integration-test: build-container run-compose
	docker-compose run --rm --name registry_test registry go test --tags=integration ./...

clean:
	docker compose down

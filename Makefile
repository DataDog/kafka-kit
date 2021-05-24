EXECUTABLES = docker go
K := $(foreach exec,$(EXECUTABLES),\
        $(if $(shell which $(exec)),some string,$(error "$(exec) not found in PATH")))

build-container:
	docker-compose build

run-compose: build-container
	docker-compose up -d

test:
	go test -v ./...

integration-test: build-container
	docker-compose run --rm --name registry_test registry go test -timeout 30s --tags=integration ./...

clean:
	docker-compose down

generate-code:
	bin/generate-code


VERSION := 0.0.4
GIT_HASH := $(shell git rev-parse HEAD)

export GO15VENDOREXPERIMENT := 1
PROTOC := protoc
DOCKER := docker
DOCKER_COMPOSE := docker-compose
DOCKER_MACHINE := docker-machine
GO := go
# Autodetect if go exists. If it does not exist, we will compile within a
# docker image. Note: Installing go is strongly recommended for contributing.
HAVE_GO := $(shell which $(GO))

export LEVEROS_DEBUG ?= 0
export LEVEROS_REPO_DIR ?= $(abspath repo)
export LEVEROS_IP_PORT ?= $(shell which $(DOCKER_MACHINE) > /dev/null && test -n "$$DOCKER_MACHINE_NAME" && docker-machine ip "$$DOCKER_MACHINE_NAME" || echo 127.0.0.1):8080
export LEVEROS_LOG_LEVEL ?= info

GO_VERSION_ARGS := -ldflags "-X main.Version=$(VERSION) -X main.GitHash=$(GIT_HASH)"
GO_BUILD_ARGS := $(GO_VERSION_ARGS) \
	$(shell test "$(LEVEROS_DEBUG)" -eq 1 && echo -race)
# Most things will run in a docker container. They need to be compiled for
# linux/amd64.
export GOOS ?= linux
export GOARCH ?= amd64
export CGO_ENABLED ?= $(LEVEROS_DEBUG)

ifdef MSVC
    UNAME_S := Windows
else
    UNAME_S := $(shell sh -c 'uname -s 2>/dev/null || echo not')
endif
ifeq ($(UNAME_S), Linux)
    HOST_OS := linux
endif
ifeq ($(UNAME_S), Darwin)
    HOST_OS := darwin
endif
ifeq ($(UNAME_S), Windows)
	HOST_OS := windows
endif
LBITS := $(shell getconf LONG_BIT)
ifeq ($(LBITS),64)
   HOST_ARCH := amd64
else
   HOST_ARCH := 386
endif

GO_TEST := GOOS=$(HOST_OS) GOARCH=$(HOST_ARCH) $(GO) test

RUN_BACKGROUND := 0
COMPOSE_BACKGROUND_ARGS := $(shell test "$(RUN_BACKGROUND)" -eq 1 && echo -d)

VENDOR_DIR := vendor
GODEPS_CONFIG := Godeps/Godeps.json

DBDATA_VOL := leveros_dbdata
ADMIN_ENV := admin.lever
MISC_PROCESSES := consul aerospike

BUILD_DIR := build
BIN_DIR := $(BUILD_DIR)/bin
CMD_DIR := cmd
SYS_TEST_DIR := systemtest
SERVICES_DIR := services
PROTOS_DIR := protos
JS_DIR := js
REPO_DIR := $(LEVEROS_REPO_DIR)
CLI_INSTALL_DIR := /usr/local/bin

CMD_SOURCES := $(wildcard $(CMD_DIR)/*/main.go)
CMD_TARGETS := $(patsubst $(CMD_DIR)/%/main.go,$(BIN_DIR)/%,$(CMD_SOURCES))

TEST_SERVICES_SOURCES := $(wildcard $(SYS_TEST_DIR)/*/main.go)
TEST_SERVICES_TARGETS := $(patsubst %/main.go,%/serve,$(TEST_SERVICES_SOURCES))

PROTO_SOURCES := $(wildcard $(PROTOS_DIR)/*/*.proto)
PROTO_TARGETS := $(patsubst $(PROTOS_DIR)/%.proto,%.pb.go,$(PROTO_SOURCES))

DOCKER_SOURCES := $(wildcard $(SERVICES_DIR)/*/Dockerfile)
DOCKER_TARGETS := $(patsubst $(SERVICES_DIR)/%/Dockerfile,docker-%,$(DOCKER_SOURCES))
DOCKER_IMAGES := $(patsubst $(SERVICES_DIR)/%/Dockerfile,leveros/%,$(DOCKER_SOURCES))

RUNNING_LEVER_CONTAINERS = $(shell $(DOCKER) ps -q --filter="label=com.leveros.islevercontainer")
LEVER_CONTAINERS = $(shell $(DOCKER) ps -a -q --filter="label=com.leveros.islevercontainer")
ALL_RUNNING_LEVER_CONTAINERS = $(shell $(DOCKER) ps -q --filter="label=com.leveros.isleveros")
ALL_LEVER_CONTAINERS = $(shell $(DOCKER) ps -a -q --filter="label=com.leveros.isleveros")

ADMIN_DIR := $(REPO_DIR)/$(ADMIN_ENV)/admin/1

.PHONY: all
all: cli docker-images

.PHONY: cli
cli: $(BIN_DIR)/lever

.PHONY: cmd
cmd: $(CMD_TARGETS)

.PHONY: pretest
pretest: vet lint fmtcheck

.PHONY: test
test: pretest
	$(GO_TEST) ./...

.PHONY: systest
systest: $(TEST_SERVICES_TARGETS)
	$(GO_TEST) -tags=integration ./$(SYS_TEST_DIR)

.PHONY: run
run:
	$(MAKE) all
	$(MAKE) runcommon

# This uses pre-built docker images as much as possible and compiles within
# docker.
.PHONY: fastrun
fastrun:
	$(MAKE) HAVE_GO="" runcommon

.PHONY: runcommon
runcommon:
	$(MAKE) \
		clean-containers \
		init-dbdata \
		admin-env
	$(DOCKER_COMPOSE) up -d --force-recreate $(MISC_PROCESSES)
	sleep 1
	$(MAKE) init-db-tables
	$(DOCKER_COMPOSE) up $(COMPOSE_BACKGROUND_ARGS) --force-recreate \
		leveroshost nghttpxext

.PHONY: install-cli
install-cli: $(BIN_DIR)/lever
	cp $< $(CLI_INSTALL_DIR)

.PHONY: uninstall-cli
uninstall-cli:
	rm $(CLI_INSTALL_DIR)/lever

.PHONY: clean
clean:
	rm -rf $(BIN_DIR)
	rm -rf $(BUILD_DIR)
	rm -f $(TEST_SERVICES_TARGETS)

.PHONY: clean-all
clean-all:
	$(MAKE) clean-all-containers
	$(MAKE) clean clean-dbdata clean-repo

.PHONY: clean-containers
clean-containers:
	-$(DOCKER) kill $(RUNNING_LEVER_CONTAINERS)
	-$(DOCKER) rm -f --volumes $(LEVER_CONTAINERS)

.PHONY: clean-all-containers
clean-all-containers:
	-$(DOCKER) kill $(ALL_RUNNING_LEVER_CONTAINERS)
	-$(DOCKER) rm -f --volumes $(ALL_LEVER_CONTAINERS)

.PHONY: clean-repo
clean-repo:
	rm -rf $(REPO_DIR)

#
# Go targets.

# The CLI is the only thing that needs to be compiled for the host OS/arch.
$(BIN_DIR)/lever: GOOS = $(HOST_OS)
$(BIN_DIR)/lever: GOARCH = $(HOST_ARCH)

GO_BUILD_COMMAND = \
	if [ -n "$(HAVE_GO)" ]; then \
		$(MAKE) GO_OUTPUT=$@ GO_MAIN_TARGET=./$< $@ ;\
	else \
		test -f $@ || $(DOCKER) run --rm \
			-v "$(PWD)":/go/src/github.com/leveros/leveros \
			-w /go/src/github.com/leveros/leveros \
			-e GOOS=$(GOOS) -e GOARCH=$(GOARCH) -e CGO_ENABLED=$(CGO_ENABLED) \
			golang:1.6.1-alpine go build $(GO_BUILD_ARGS) -o $@ ./$< ;\
	fi

$(BIN_DIR)/%: $(CMD_DIR)/%/main.go $(PROTO_TARGETS) $(BIN_DIR) FORCE
	$(GO_BUILD_COMMAND)

#
# Go system test targets.

$(SYS_TEST_DIR)/%/serve: $(SYS_TEST_DIR)/%/main.go $(PROTO_TARGETS) FORCE
	$(GO_BUILD_COMMAND)

#
# Go pretest targets.

FIND_GO_FILES := find -type f -name '*.go' \
	! -path './vendor/*' ! -path './.git/*' ! -name '*.pb.go'

.PHONY: vet
vet:
	$(FIND_GO_FILES) -exec go tool vet {} \;

.PHONY: lint
lint:
	$(FIND_GO_FILES) -exec golint {} \;

.PHONY: fmt
fmt:
	$(FIND_GO_FILES) -exec gofmt -s -w {} \;

.PHONY: fmtcheck
fmtcheck:
	@ export output="$$($(FIND_GO_FILES) -exec gofmt -s -d {} \;)"; \
		test -z "$${output}" || (echo "$${output}" && exit 1)

#
# Source generation targets.

PROTOC_CMD = $(PROTOC) -I $(dir $<) --go_out=plugins=grpc:$(dir $@) $<

.SECONDARY: $(PROTO_TARGETS)

core/%.pb.go: $(PROTOS_DIR)/core/%.proto
	$(PROTOC_CMD)

fleettracker/%.pb.go: $(PROTOS_DIR)/fleettracker/%.proto
	$(PROTOC_CMD)

hostman/%.pb.go: $(PROTOS_DIR)/hostman/%.proto
	$(PROTOC_CMD)

#
# JS targets.

$(JS_DIR)/leveros-common/leverrpc.proto: $(PROTOS_DIR)/core/leverrpc.proto
	cp $< $@
$(JS_DIR)/leveros-common/LICENSE.md: LICENSE.md
	cp $< $@
$(JS_DIR)/leveros-server/LICENSE.md: LICENSE.md
	cp $< $@
$(JS_DIR)/leveros/LICENSE.md: LICENSE.md
	cp $< $@

.PHONY: jsprep-leveros-common
jsprep-leveros-common: $(JS_DIR)/leveros-common/leverrpc.proto
jsprep-leveros-common: $(JS_DIR)/leveros-common/LICENSE.md

.PHONY: jsprep-leveros-server
jsprep-leveros-server: $(JS_DIR)/leveros-server/LICENSE.md

.PHONY: jsprep-leveros
jsprep-leveros: $(JS_DIR)/leveros/LICENSE.md

.PHONY: npm-publish
npm-publish: jsprep-leveros-common jsprep-leveros
	cd $(JS_DIR)/leveros-common && grunt lint && grunt
	cd $(JS_DIR)/leveros && grunt lint && grunt
	cd $(JS_DIR)/leveros-common && npm publish
	cd $(JS_DIR)/leveros && npm publish

#
# Docker images targets.

RSYNC_JS = rsync -a --delete \
	--exclude node_modules/ \
	--exclude compiled/ \
	--exclude npm-debug.log

.PHONY: docker-images
docker-images: $(DOCKER_TARGETS)

docker-%: $(SERVICES_DIR)/%/Dockerfile FORCE
	$(DOCKER) build -t leveros/$(@:docker-%=%) $(dir $<)

docker-consul: | docker-base
docker-nghttpx: | docker-base
docker-levercontainer: | docker-base
docker-levercontainer: $(SERVICES_DIR)/levercontainer/js/leveros-server
docker-levercontainer: $(SERVICES_DIR)/levercontainer/js/leveros-common
$(SERVICES_DIR)/levercontainer/js/leveros-server: $(JS_DIR)/leveros-server FORCE
	$(MAKE) jsprep-leveros-server
	$(RSYNC_JS) $< $(dir $@)
$(SERVICES_DIR)/levercontainer/js/leveros-common: $(JS_DIR)/leveros-common FORCE
	$(MAKE) jsprep-leveros-common
	$(RSYNC_JS) $< $(dir $@)
docker-leveroshost: $(SERVICES_DIR)/leveroshost/leveroshost | docker-base
$(SERVICES_DIR)/leveroshost/leveroshost: $(BIN_DIR)/leveroshost
	cp $< $@

.PHONY: push-docker-images
push-docker-images: $(DOCKER_TARGETS)
	for image in $(DOCKER_IMAGES); do \
		$(DOCKER) push $$image ;\
	done

.PHONY: pull-docker-images
pull-docker-images:
	for image in $(DOCKER_IMAGES); do \
		$(DOCKER) pull $$image ;\
	done

#
# DB state targets.

.PHONY: init-dbdata
init-dbdata:
	$(DOCKER) run --rm -v $(DBDATA_VOL):/leveros/dbdata \
	    leveros/aerospikedev \
	    bash -c "chown -R aerospike:aerospike /leveros/dbdata"

.PHONY: clean-dbdata
clean-dbdata:
	-$(DOCKER) volume rm $(DBDATA_VOL)

.PHONY: init-db-tables
init-db-tables: $(BIN_DIR)/inittables
	./runasdocker.sh $<

#
# Admin bootstrap targets.

.PHONY: admin-env
admin-env: $(ADMIN_DIR) $(ADMIN_DIR)/serve $(ADMIN_DIR)/lever.json

$(ADMIN_DIR)/serve: $(BIN_DIR)/adminservice | $(ADMIN_DIR)
	cp $< $@

$(ADMIN_DIR)/lever.json: $(CMD_DIR)/adminservice/lever.json | $(ADMIN_DIR)
	cp $< $@

$(ADMIN_DIR):
	mkdir -p $@

#
# Misc targets.

.PHONY: FORCE
FORCE:

$(BUILD_DIR):
	mkdir -p $@

$(BIN_DIR):
	mkdir -p $@

$(REPO_DIR):
	mkdir -p $@
	chmod o+rwx $@

#
# Targets related to compiling go sources. Automatically detects dependencies
# using go list. These are invoked with another call to make like so:
#
# $(MAKE) \
#     GO_OUTPUT=<output> \
#     GO_MAIN_TARGET=./<target> \
#     <output>

GO_MAIN_TARGET :=
GO_OUTPUT :=

ifneq (,$(GO_MAIN_TARGET))

PACKAGE := $(dir $(GO_MAIN_TARGET))
IMPORT_PATH := $(shell $(GO) list .)

remove_brackets = $(subst [,,$(subst ],,$(1)))
get_deps = $(shell $(GO) list $(1)) $(call remove_brackets,$(shell $(GO) list -f '{{.Deps}}' $(1)))
get_own_deps = $(filter-out $(IMPORT_PATH)/vendor/%,$(filter $(IMPORT_PATH)/%,$(call get_deps,$(1))))
get_package_files = $(addprefix $(shell $(GO) list -f '{{.Dir}}' $(1))/,$(call remove_brackets,$(shell $(GO) list -f '{{.GoFiles}}' $(1))))
OWN_PACKAGE_DEPS := $(call get_own_deps,$(PACKAGE))
GO_DEPS := $(foreach package_dep,$(OWN_PACKAGE_DEPS),$(call get_package_files,$(package_dep)))

$(GO_OUTPUT): $(GO_DEPS) $(VENDOR_DIR) $(GODEPS_CONFIG)
	GOOS=$(GOOS) GOARCH=$(GOARCH) CGO_ENABLED=$(CGO_ENABLED) \
		$(GO) build $(GO_BUILD_ARGS) -o $@ $(GO_MAIN_TARGET)

endif  # ifneq (,$(GO_MAIN_TARGET))

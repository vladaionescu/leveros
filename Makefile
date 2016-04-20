
export GO15VENDOREXPERIMENT := 1
GO := go
PROTOC := protoc
DOCKER := docker
DOCKER_COMPOSE := docker-compose

GO_BUILD_ARGS := -race
# Most things will run in a docker container. They need to be compiled for
# linux/amd64.
GOOS := linux
GOARCH := amd64

VENDOR_DIR := vendor
GODEPS_CONFIG := Godeps/Godeps.json

export LEVEROS_REPO_DIR ?= $(abspath repo)
export LEVEROS_LISTEN_IP_PORT ?= 127.0.0.1:8080
DBDATA_VOL := leveros_dbdata
ADMIN_ENV := admin.lever
MISC_PROCESSES := consul aerospike
LEVEROSHOST_CONFIG := services/leveroshost/dev.config.json

BUILD_DIR := build
BIN_DIR := $(BUILD_DIR)/bin
CMD_DIR := cmd
SYS_TEST_DIR := systemtest
SERVICES_DIR := services
PROTOS_DIR := protos
REPO_DIR := $(LEVEROS_REPO_DIR)

CMD_SOURCES := $(wildcard $(CMD_DIR)/*/main.go)
CMD_TARGETS := $(patsubst $(CMD_DIR)/%/main.go,$(BIN_DIR)/%,$(CMD_SOURCES))

TEST_SERVICES_SOURCES := $(wildcard $(SYS_TEST_DIR)/*/main.go)
TEST_SERVICES_TARGETS := $(patsubst %/main.go,%/serve,$(TEST_SERVICES_SOURCES))

PROTO_SOURCES := $(wildcard $(PROTOS_DIR)/*/*.proto)
PROTO_TARGETS := $(patsubst $(PROTOS_DIR)/%.proto,%.pb.go,$(PROTO_SOURCES))

DOCKER_SOURCES := $(wildcard $(SERVICES_DIR)/*/Dockerfile)
DOCKER_TARGETS := $(patsubst $(SERVICES_DIR)/%/Dockerfile,docker-%,$(DOCKER_SOURCES))

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
	$(GO) test ./...

.PHONY: systest
systest: $(TEST_SERVICES_TARGETS)
	$(GO) test -tags=integration ./$(SYS_TEST_DIR)

.PHONY: run
run:
	$(MAKE) \
		clean-containers \
		init-dbdata \
		docker-images \
		cli \
		admin-env
	$(DOCKER_COMPOSE) up -d --force-recreate $(MISC_PROCESSES)
	sleep 1
	$(MAKE) \
		upload-config \
		init-db-tables
	$(DOCKER_COMPOSE) up --force-recreate leveroshost

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

# The CLI is the only thing that needs to be compiled for the current OS/arch.
$(BIN_DIR)/lever: GOOS:="" GOARCH:=""

$(BIN_DIR)/%: $(CMD_DIR)/%/main.go $(PROTO_TARGETS) $(BIN_DIR) FORCE
	export > /dev/null ; $(MAKE) GO_OUTPUT=$@ GO_MAIN_TARGET=./$< $@

#
# Go system test targets.

$(SYS_TEST_DIR)/%/serve: $(SYS_TEST_DIR)/%/main.go $(PROTO_TARGETS) FORCE
	export > /dev/null ; $(MAKE) GO_OUTPUT=$@ GO_MAIN_TARGET=./$< $@

#
# Go pretest targets.

FIND_GO_FILES := find -type f -name '*.go' ! -path './vendor/*' ! -path './.git/*' ! -name '*.pb.go'

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
GRPC_IMPORT_REPLACE_CMD = sed -i 's|grpc "google.golang.org/grpc"|grpc "github.com/leveros/grpc-go"|g' $@

.SECONDARY: $(PROTO_TARGETS)

core/%.pb.go: $(PROTOS_DIR)/core/%.proto
	$(PROTOC_CMD)
	$(GRPC_IMPORT_REPLACE_CMD)

fleettracker/%.pb.go: $(PROTOS_DIR)/fleettracker/%.proto
	$(PROTOC_CMD)
	$(GRPC_IMPORT_REPLACE_CMD)

hostman/%.pb.go: $(PROTOS_DIR)/hostman/%.proto
	$(PROTOC_CMD)
	$(GRPC_IMPORT_REPLACE_CMD)

#
# Docker images targets.

.PHONY: docker-images
docker-images: $(DOCKER_TARGETS)

docker-%: $(SERVICES_DIR)/%/Dockerfile FORCE
	$(DOCKER) build -t leveros/$(@:docker-%=%) $(dir $<)

docker-consul: | docker-ubuntubase
docker-leveroshost: | docker-ubuntubase
docker-leveroshost: $(SERVICES_DIR)/leveroshost/leveroshost
$(SERVICES_DIR)/leveroshost/leveroshost: $(BIN_DIR)/leveroshost
	cp $< $@

#
# DB state targets.

.PHONY: init-dbdata
init-dbdata: docker-aerospikedev
	$(DOCKER) run --rm -v $(DBDATA_VOL):/leveros/dbdata \
	    leveros/aerospikedev \
	    bash -c "chown -R aerospike:aerospike /leveros/dbdata"

.PHONY: clean-dbdata
clean-dbdata:
	-$(DOCKER) volume rm $(DBDATA_VOL)

.PHONY: init-db-tables
init-db-tables: $(BIN_DIR)/inittables
	./runasdocker.sh $<

.PHONY: upload-config
upload-config: $(BIN_DIR)/uploadconfig
	EXTRA_DOCKER_ARGS="-v $(abspath $(LEVEROSHOST_CONFIG)):/leveros/$(notdir $(LEVEROSHOST_CONFIG))" \
		./runasdocker.sh $< \
		--file "/leveros/$(notdir $(LEVEROSHOST_CONFIG))" \
	    --service leveroshost

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

#
# Targets related to compiling go sources. Automatically detects dependencies
# using go list. These are invoked with another call to make like so:
#
# $(MAKE) GO_BUILD_ARGS=$(GO_BUILD_ARGS) \
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
	$(GO) build $(GO_BUILD_ARGS) -o $@ $(GO_MAIN_TARGET)

endif  # ifneq (,$(GO_MAIN_TARGET))

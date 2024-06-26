# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOGET=$(GOCMD) mod
BINARY_NAME=./bin/redis_agent
BINARY_UNIX=$(BINARY_NAME)_x86_64
REDIS_PKG=RedisAgent
BUILDTS := $(shell date '+%Y-%m-%d %H:%M:%S')
GITHASH := $(shell git rev-parse HEAD)
GITBRANCH := $(shell git rev-parse --abbrev-ref HEAD)
GOVERSION := $(shell go version)
LDFLAGS += -X "$(REDIS_PKG)/api.BuildTS=$(BUILDTS)"
LDFLAGS += -X "$(REDIS_PKG)/api.GitHash=$(GITHASH)"
LDFLAGS += -X "$(REDIS_PKG)/api.GitBranch=$(GITBRANCH)"
LDFLAGS += -X "$(REDIS_PKG)/api.GoVersion=$(GOVERSION)"

all: myloader mydumper
build:
	$(GOBUILD)  -ldflags '$(LDFLAGS) -s -w'  -o $(BINARY_NAME)
mod:
	$(GOCMD) mod tidy
clean:
	$(GOCLEAN)
	rm -f $(BINARY_UNIX)
	rm -f $(BINARY_NAME)
run:
	$(GOBUILD)  -ldflags '$(LDFLAGS) -s -w' -o $(BINARY_NAME)
	./$(BINARY_NAME)
myloader:

mydumper:

# Cross compilation
linux:
	CGO_ENABLED=1 GOOS=linux GOARCH=amd64 $(GOBUILD)  -ldflags '$(LDFLAGS) -s -w' -o $(BINARY_UNIX)

windows:

mac:

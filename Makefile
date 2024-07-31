# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOGET=$(GOCMD) mod
MYDUMPER_BUILD=cmd/mydumper.go
MYLOADER_BUILD=cmd/myloader.go
MYDUMPER_BINARY_NAME=cmd/mydumper
MYLOADER_BINARY_NAME=cmd/myloader
MYDUMPER_BINARY_UNIX=$(MYDUMPER_BINARY_NAME)_x86_64
MYLOADER_BINARY_UNIX=$(MYLOADER_BINARY_NAME)_x86_64
PKG_NAME=go-mydumper
BUILDTS := $(shell date '+%Y-%m-%d %H:%M:%S')
GITHASH := $(shell git rev-parse HEAD)
GITBRANCH := $(shell git rev-parse --abbrev-ref HEAD)
GOVERSION := $(shell go version)
LDFLAGS = -X "$(PKG_NAME)/src.BuildTS=$(BUILDTS)"
LDFLAGS += -X "$(PKG_NAME)/src.GitHash=$(GITHASH)"
LDFLAGS += -X "$(PKG_NAME)/src.GitBranch=$(GITBRANCH)"
LDFLAGS += -X "$(PKG_NAME)/src.GoVersion=$(GOVERSION)"

all: build

build:
	$(GOBUILD)  -ldflags '$(LDFLAGS) -s -w'  -o $(MYDUMPER_BINARY_NAME) $(MYDUMPER_BUILD)
	$(GOBUILD)  -ldflags '$(LDFLAGS) -s -w'  -o $(MYLOADER_BINARY_NAME) $(MYLOADER_BUILD)

mod:
	$(GOCMD) mod tidy

clean:
	$(GOCLEAN)
	rm -f $(MYDUMPER_BINARY_NAME) $(MYLOADER_BINARY_NAME) $(MYDUMPER_BINARY_UNIX)  $(MYLOADER_BINARY_UNIX)

run:
	$(GOBUILD)  -ldflags '$(LDFLAGS) -s -w' -o $(MYDUMPER_BINARY_NAME) $(MYDUMPER_BUILD)
	./$(MYDUMPER_BINARY_NAME) --version

.PHONY: myloader
myloader:
	$(GOBUILD)  -ldflags '$(LDFLAGS) -s -w' -o $(MYLOADER_BINARY_NAME) $(MYLOADER_BUILD)

.PHONY: mydumper
mydumper:
	$(GOBUILD)  -ldflags '$(LDFLAGS) -s -w' -o $(MYDUMPER_BINARY_NAME) $(MYDUMPER_BUILD)

# Cross compilation
linux:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GOBUILD)  -ldflags '$(LDFLAGS) -s -w' -o $(MYDUMPER_BINARY_UNIX) $(MYDUMPER_BUILD)
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GOBUILD)  -ldflags '$(LDFLAGS) -s -w' -o $(MYLOADER_BINARY_UNIX) $(MYLOADER_BUILD)

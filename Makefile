# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOGET=$(GOCMD) mod
MYDUMPER_BUILD=cmd/mydumper.go
MYLOADER_BUILD=cmd/myloader.go
MYDUMPER=mydumper
MYLOADER=myloader
OUTPUT=cmd
MYDUMPER_BINARY_NAME=$(OUTPUT)/$(MYDUMPER)_darwin_for_x86_64
MYLOADER_BINARY_NAME=$(OUTPUT)/$(MYLOADER)_darwin_for_x86_64
MYDUMPER_BINARY_UNIX=$(OUTPUT)/$(MYDUMPER)_linux_for_x86_64
MYLOADER_BINARY_UNIX=$(OUTPUT)/$(MYDUMPER)_linux_for_x86_64
MYDUMPER_BINARY_WIN=$(OUTPUT)/$(MYDUMPER)_windows_for_x86_64.exe
MYLOADER_BINARY_WIN=$(OUTPUT)/$(MYDUMPER)_windows_for_x86_64.exe
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

windows:
	CGO_ENABLED=1 GOOS=windows GOARCH=amd64 $(GOBUILD)  -ldflags '$(LDFLAGS) -s -w' -o $(MYDUMPER_BINARY_WIN) $(MYDUMPER_BUILD)
	CGO_ENABLED=1 GOOS=windows GOARCH=amd64 $(GOBUILD)  -ldflags '$(LDFLAGS) -s -w' -o $(MYLOADER_BINARY_WIN) $(MYDUMPER_BUILD)

darwin:
	CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 $(GOBUILD)  -ldflags '$(LDFLAGS) -s -w' -o $(MYDUMPER_BINARY_NAME) $(MYDUMPER_BUILD)
	CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 $(GOBUILD)  -ldflags '$(LDFLAGS) -s -w' -o $(MYLOADER_BINARY_NAME) $(MYDUMPER_BUILD)

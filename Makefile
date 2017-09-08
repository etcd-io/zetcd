export PATH := $(PWD)/bin:$(PATH)

VERSION ?= $(shell ./scripts/git-version)
SHA ?= $(shell git rev-parse HEAD)

DOCKER_IMAGE = coreos/zetcd:$(VERSION)

$(shell mkdir -p bin)

export GOBIN=$(PWD)/bin
export PATH=$(GOBIN):$(shell printenv PATH)
export VERSION
export SHA

release-binary:
	./scripts/release-binary

bin/zetcd-release:
	./scripts/docker-build

docker-image: bin/zetcd-release
	docker build -t $(DOCKER_IMAGE) .

clean:
	rm -rf bin/

test:
	go get -t -v ./...
	go test -v -race ./integration

SRC=$(shell find -name \*.go | grep -v vendor | cut -f2- -d'/')

# go get honnef.co/go/tools/simple
# go get honnef.co/go/tools/unused
# go get honnef.co/go/tools/staticheck
FMT = fmt-gosimple fmt-unused fmt-staticcheck fmt-fmt fmt-vet
fmt: $(FMT)

fmt-gosimple:
	! ( gosimple ./... 2>&1 | grep -v vendor | grep : )
fmt-unused:
	! ( unused ./... 2>&1 | grep -v vendor | grep : )
fmt-staticcheck:
	! ( staticcheck ./... 2>&1 | grep -v vendor | grep : )
fmt-vet:
	! ( go tool vet $(shell go tool vet --help 2>&1 | awk ' { print $1 } ' | grep "^-" -- ) $(SRC) 2>&1 | grep -v vendor | grep : )
	! ( go vet ./... 2>&1 | grep -v vendor | grep : )

fmt-fmt:
	! ( gofmt -l -s -d $(SRC) | grep -A10 diff )

FORCE:

.PHONY: release-binary docker-image clean test fmt $(FMT)

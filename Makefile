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

FORCE:

.PHONY: release-binary docker-image clean

export PATH := $(PWD)/bin:$(PATH)

VERSION ?= $(shell ./scripts/git-version)

DOCKER_IMAGE = coreos/zetcd:$(VERSION)

$(shell mkdir -p bin)

export GOBIN=$(PWD)/bin
export PATH=$(GOBIN):$(shell printenv PATH)

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
